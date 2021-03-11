#lang racket/base

(require racket/contract/base
         racket/contract/region
         racket/function
         racket/match
         struct-plus-plus
         thread-with-id)

(provide start-majordomo
         stop-majordomo
         majordomo?
         majordomo.id

         current-task
         task++  task
         task.id       task-id
         task.status   task-status
         task.data     task-data
         task?
         task-status/c

         update-data
         keepalive
         success
         failure
         add-task)

(define-logger md)

;;----------------------------------------------------------------------

(struct++ majordomo
          ([(id    (gensym "majordomo-"))  symbol?] ; makes it human-identifiable
           [(cust  (make-custodian))       custodian?]))

(define task-status/c (or/c 'success 'failure 'unspecified 'timeout))
(struct++ task
          ([(id     (gensym "task-"))   symbol?]
           [(status 'unspecified)       task-status/c]
           [(data   (hash))             any/c]
           ; private fields
           [(manager-ch (make-channel)) channel?]))

(define/contract current-task
  (parameter/c (or/c #f task?))
  (make-parameter #f #f 'current-task))

;;----------------------------------------------------------------------

(define (start-majordomo)
  (log-md-debug "~a: starting majordomo..." (thread-id))
  (majordomo++))

;;----------------------------------------------------------------------

(define/contract (stop-majordomo jarvis)
  (-> majordomo? any)
  (log-md-debug "~a: stopping majordomo..." (thread-id))
  (custodian-shutdown-all (majordomo.cust jarvis)))

;;----------------------------------------------------------------------

(define finalized? (make-parameter #f))

(define (update-data data)
  (log-md-debug "~a: update-data with ~v" (thread-id) data)
  (define the-task (current-task))
  (current-task (set-task-data the-task data))
  (channel-put (task.manager-ch the-task) (list 'update-data data)))

(define (keepalive)
  (log-md-debug "~a: keepalive" (thread-id))
  (channel-put (task.manager-ch (current-task)) 'keepalive))

(define (success [data the-unsupplied-arg])
  (log-md-debug "~a: success! data is: ~v" (thread-id) data)
  (finalized? #t)
  (finish-with 'success
               (if (unsupplied-arg? data)
                   (current-task)
                   (set-task-data (current-task) data))))

(define (failure [data the-unsupplied-arg])
  (log-md-debug "~a: failure! data is: ~v" (thread-id) data)
  (finalized? #t)
  (finish-with 'failure
               (if (unsupplied-arg? data)
                   (current-task)
                   (set-task-data (current-task) data))))

(define/contract (finish-with status the-task)
  (-> symbol? task? any)
  (log-md-debug "~a: finish-with status: ~v" (thread-id) status)
  (channel-put (task.manager-ch the-task)
               (list status the-task)))

;;----------------------------------------------------------------------

(define (finalize the-task result-ch
                  #:sort-op           sort-op
                  #:sort-key          sort-key
                  #:sort-cache-keys?  cache-keys?
                  #:filter            filter-func
                  #:pre               pre
                  #:post              post)
  (log-md-debug "~a: entering finalize" (thread-id))
  (define raw-data (task.data the-task))

  (log-md-debug "~a: raw data is: ~v" (thread-id) raw-data)
  (log-md-debug "~a: filter func is: ~a" (thread-id) filter-func)

  (channel-put
   result-ch
   (with-handlers ([any/c (λ (e)
                            (define t (or (current-task) (task++)))
                            (set-task-status (set-task-data t e)
                                             'failure))])
     (define filtered-data
       (match filter-func
         [#f raw-data]
         [_  (filter filter-func raw-data)]))

     (log-md-debug "~a: filtered data is: ~v" (thread-id) filtered-data)

     (channel-put result-ch
                  (set-task-data the-task
                                 (post
                                  (let ([data (pre filtered-data)])
                                    (cond [sort-op (sort data          sort-op
                                                         #:key         sort-key
                                                         #:cache-keys? cache-keys?)]
                                          [else    data]))))))))


;;----------------------------------------------------------------------

(define/contract (add-task jarvis action
                           #:keepalive         [keepalive   5]
                           #:retries           [retries     3]
                           #:parallel?         [parallel?   #f]
                           #:unwrap?           [unwrap?     #f]
                           #:filter            [filter-func #f]
                           #:pre               [pre         identity]
                           #:sort-op           [sort-op     #f]
                           #:sort-key          [sort-key    identity]
                           #:sort-cache-keys?  [cache-keys? #f]
                           #:post              [post       identity]
                           . args)
  (->* (majordomo? (unconstrained-domain-> any/c))
       (#:keepalive         (and/c real? (not/c negative?))
        #:retries           (or/c natural-number/c +inf.0)
        #:parallel?         boolean?
        #:unwrap?            boolean?
        #:filter            (or/c #f procedure?)
        #:pre               procedure?
        #:sort-op           (or/c #f (-> any/c any/c any/c))
        #:sort-key          (-> any/c any/c)
        #:sort-cache-keys?  boolean?
        #:post              procedure?)
       #:rest list?
       channel?)

  (define result-ch (make-channel))
  (cond [parallel?
         ; Spawn each argument off into its own task which will run in
         ; its own thread.

         ;  NOTE: In some cases it will be easier for the customer to pass args as a list
         ;  instead of as separate arguments, for example when the args are generated via
         ;  a 'map'.  In that case they can use #:unwrap? #t to have us unwrap it for them.
         (define subtask-channels
           (for/list ([arg (in-list (if unwrap? (car args) args))])
             (add-task-helper jarvis
                              action
                              (list arg)
                              (make-channel)
                              #:keepalive         keepalive
                              #:retries           retries
                              #:parallel?         #f)))

         ; Collect all the results from the subtasks
         (define raw-data (map (compose task.data sync) subtask-channels))

         ; Finalize them in another thread, since finalize uses
         ; channel-put, which is blocking
         (thread-with-id (thunk (finalize (task++ #:data raw-data)
                                          result-ch
                                          #:sort-op           sort-op
                                          #:sort-key          sort-key
                                          #:sort-cache-keys?  cache-keys?
                                          #:filter            filter-func
                                          #:pre               pre
                                          #:post              post)))
         ; return the result channel
         result-ch]
        [else
         (add-task-helper jarvis
                          action
                          args
                          result-ch
                          #:keepalive         keepalive
                          #:retries           retries
                          #:parallel?         parallel?
                          #:unwrap?            unwrap?
                          #:sort-op           sort-op
                          #:sort-key          sort-key
                          #:sort-cache-keys?  cache-keys?
                          #:filter            filter-func
                          #:pre               pre
                          #:post              post)]))


(define/contract (add-task-helper jarvis action args result-ch
                                  #:keepalive         [keepalive    5]
                                  #:retries           [retries      3]
                                  #:parallel?         [parallel?    #f]
                                  #:unwrap?           [unwrap?      #f]
                                  #:filter            [filter-func  #f]
                                  #:pre               [pre          identity]
                                  #:sort-op           [sort-op      #f]
                                  #:sort-key          [sort-key     identity]
                                  #:sort-cache-keys?  [cache-keys?  #f]
                                  #:post              [post         identity])
  (->* (majordomo? (unconstrained-domain-> any/c) list? channel?)
       (#:keepalive         (and/c real? (not/c negative?))
        #:retries           (or/c exact-nonnegative-integer? +inf.0)
        #:parallel?         boolean?
        #:unwrap?            boolean?
        #:sort-op           (or/c #f (-> any/c any/c any/c))
        #:sort-key          (-> any/c any/c)
        #:sort-cache-keys?  boolean?
        #:filter            (or/c #f procedure?)
        #:pre               procedure?
        #:post              procedure?)
       channel?)

  (parameterize ([current-custodian (majordomo.cust jarvis)])
    (match-define (and the-task (struct* task ([manager-ch manager-ch])))
      (task++))

    ; worker
    (define (start-worker the-task)
      (parameterize ([current-custodian (make-custodian)]
                     [current-task      the-task])
        (thread
         (thunk
          (with-handlers ([any/c failure])
            (log-md-debug "~a: about to apply action" (thread-id))

            ; The arguments are passed into a rest arg, meaning that they come to as a
            ; list and we therefore need to use 'apply' to unwrap them so that the action
            ; can get them as individual items.
            ;
            ; In some cases the function expects individual arguments but it's more
            ; convenient for the customer to pass it as a list, e.g. because the args were
            ; generated via a 'map'.  In this case we need to unwrap it twice and we
            ; expect that 'args' is a one-element list where the element is a list
            ; containing the actual args.
            ;
            ; Example:
            ;
            ;   (define (foo a b c) (+ a b c))
            ;   (add-task jarvis foo 1 2 3)     ; the 'args' binding contains '(1 2 3)
            ;   (add-task jarvis foo '(1 2 3))  ; the 'args' binding contains '((1 2 3))
            ;
            (define result (apply action (if unwrap? (car args) args)))
            (when (not (finalized?))
              (success result)))))))

    (define worker (start-worker the-task))

    ; manager
    (thread-with-id
     (thunk
      (let loop ([retries  retries]
                 [the-task the-task]
                 [worker   worker])
        (match (sync/timeout keepalive manager-ch worker)
          ['keepalive
           (loop retries the-task worker)]
          ;
          [(list 'update-data data)
           (loop retries (set-task-data the-task data) worker)]
          ;
          [(list result the-task)
           (finalize (set-task-status the-task result) result-ch
                     #:sort-op           sort-op
                     #:sort-key          sort-key
                     #:sort-cache-keys?  cache-keys?
                     #:filter            filter-func
                     #:pre               pre
                     #:post              post)]
          ;
          [(and value (or (== worker) #f))
           #:when (> retries 0) ; timeout or thread died, can be retried
           (kill-thread worker)
           (loop (sub1 retries)
                 the-task
                 (start-worker the-task))]
          ;
          [(and value (or (== worker) #f)) ; timeout or thread died, no retries left
           (kill-thread worker)
           (finalize (set-task-status the-task 'timeout) result-ch
                     #:sort-op           sort-op
                     #:sort-key          sort-key
                     #:sort-cache-keys?  cache-keys?
                     #:filter            filter-func
                     #:pre               pre
                     #:post              post)])))))
  result-ch)
