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
  (parameter/c task?)
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
                  #:pre               pre
                  #:post              post)
  (log-md-debug "~a: entering finalize" (thread-id))
  (define raw-data (task.data the-task))

  (channel-put result-ch
               (set-task-data the-task
                              (post
                               (let ([data (pre raw-data)])
                                 (cond [sort-op (sort data          sort-op
                                                      #:key         sort-key
                                                      #:cache-keys? cache-keys?)]
                                       [else    data]))))))
  (log-md-debug "~a: raw data is: ~v" (thread-id) raw-data)
  (log-md-debug "~a: filter func is: ~a" (thread-id) filter-func)

;;----------------------------------------------------------------------

(define/contract (add-task jarvis action
                           #:keepalive         [keepalive   5]
                           #:retries           [retries     3]
                           #:parallel?         [parallel?   #f]
                           #:unwrap?           [unwrap?     #f]
                           #:pre               [pre         identity]
                           #:sort-op           [sort-op     #f]
                           #:sort-key          [sort-key    identity]
                           #:sort-cache-keys?  [cache-keys? #f]
                           #:post              [post        identity]
                           . args)
  (->* (majordomo? (unconstrained-domain-> any/c))
       (#:keepalive         (and/c real? (not/c negative?))
        #:retries           (or/c natural-number/c +inf.0)
        #:parallel?         boolean?
        #:pre               procedure?
        #:sort-op           (or/c #f (-> any/c any/c any/c))
        #:sort-key          (-> any/c any/c)
        #:sort-cache-keys?  boolean?
        #:post              procedure?)
       #:rest list?
       channel?)
  (log-md-debug "~a: entering add-task" (thread-id))
  (define result-ch (make-channel))
  (cond [parallel?
         (log-md-debug "~a: running in parallel" (thread-id))
         ; Spawn each argument off into its own task which will run in
         ; its own thread.
         (define subtask-channels
           (for/list ([arg (in-list args)])
             (add-task-helper jarvis
                              action
                              (list arg)
                              (make-channel)
                              #:keepalive         keepalive
                              #:retries           retries
                              #:parallel?         #f)))
         (log-md-debug "~a: after subtask-channels generated" (thread-id))

         ; Collect all the results from the subtasks
         (define raw-data (map (compose task.data sync) subtask-channels))
         (log-md-debug "~a: in add-task. raw-data is: ~v" (thread-id)  raw-data)

         ; Finalize them in another thread, since finalize uses
         ; channel-put, which is blocking
         (thread (thunk (finalize (task++ #:data raw-data)
                                  result-ch
                                  #:sort-op           sort-op
                                  #:sort-key          sort-key
                                  #:sort-cache-keys?  cache-keys?
                                  #:pre               pre
                                  #:post              post)))
         ; return the result channel
         result-ch]
        [else
         (log-md-debug "~a: in add-task. running in series" (thread-id))
         (add-task-helper jarvis
                          action
                          args
                          result-ch
                          #:keepalive         keepalive
                          #:retries           retries
                          #:parallel?         parallel?
                          #:sort-op           sort-op
                          #:sort-key          sort-key
                          #:sort-cache-keys?  cache-keys?
                          #:pre               pre
                          #:post              post)]))


(define/contract (add-task-helper jarvis action args result-ch
                                  #:keepalive         [keepalive   5]
                                  #:retries           [retries     3]
                                  #:parallel?         [parallel?   #f]
                                  #:sort-op           [sort-op     #f]
                                  #:sort-key          [sort-key    identity]
                                  #:sort-cache-keys?  [cache-keys? #f]
                                  #:filter            [filter-func #f]
                                  #:pre               [pre         identity]
                                  #:post              [post        identity])
  (->* (majordomo? (unconstrained-domain-> any/c) list? channel?)
       (#:keepalive         (and/c real? (not/c negative?))
        #:retries           (or/c exact-nonnegative-integer? +inf.0)
        #:parallel?         boolean?
        #:sort-op           (or/c #f (-> any/c any/c any/c))
        #:sort-key          (-> any/c any/c)
        #:sort-cache-keys?  boolean?
        #:pre               procedure?
        #:post              procedure?)
       channel?)
  (log-md-debug "~a: entering add-task-helper with args ~v" (thread-id) args)

  (parameterize ([current-custodian (majordomo.cust jarvis)])
    (match-define (and the-task (struct* task ([manager-ch manager-ch])))
      (task++))

    ; worker
    (define (start-worker the-task)
      (log-md-debug "~a: entering start-worker" (thread-id))

      (parameterize ([current-custodian (make-custodian)]
                     [current-task      the-task])
        (thread
         (thunk
          (log-md-debug "~a: starting the worker thread" (thread-id))
          (with-handlers ([any/c failure])
            (define result (apply action args))
            (when (not (finalized?))
              (log-md-debug "~a: was not finalized.  finalizing via 'success'" (thread-id))
              (success result)))))))

    (define worker (start-worker the-task))

    ; manager
    (thread
     (thunk
      (log-md-debug "~a: starting the manager thread" (thread-id))
      (let loop ([retries  retries]
                 [the-task the-task]
                 [worker   worker])
        (log-md-debug "~a: in manager thread. retries: ~a" (thread-id) retries)
        (match (sync/timeout keepalive manager-ch worker)
          ['keepalive
           (log-md-debug "~a: in manager thread. keepalive" (thread-id))
           (loop retries the-task worker)]
          ;
          [(list 'update-data data)
           (log-md-debug "~a: in manager thread. update-data" (thread-id))
           (loop retries (set-task-data the-task data) worker)]
          ;
          [(list result the-task)
           (log-md-debug "~a: in manager thread. finalize" (thread-id))
           (finalize (set-task-status the-task result) result-ch
                     #:sort-op           sort-op
                     #:sort-key          sort-key
                     #:sort-cache-keys?  cache-keys?
                     #:pre               pre
                     #:post              post)]
          ;
          [(and value (or (== worker) #f))
           #:when (> retries 0) ; timeout or thread died, can be retried
           (log-md-debug "~a: retrying" (thread-id))
           (kill-thread worker)
           (loop (sub1 retries)
                 the-task
                 (start-worker the-task))]
          ;
          [(and value (or (== worker) #f)) ; timeout or thread died, no retries left
           (log-md-debug "~a: thread timeout" (thread-id))
           (kill-thread worker)
           (finalize (set-task-status the-task 'timeout) result-ch
                     #:sort-op           sort-op
                     #:sort-key          sort-key
                     #:sort-cache-keys?  cache-keys?
                     #:pre               pre
                     #:post              post)])))))
  (log-md-debug "~a: leaving add-task-helper" (thread-id))
  result-ch)
