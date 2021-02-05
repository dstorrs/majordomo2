#lang racket/base

(require racket/contract/base
         racket/contract/region
         racket/function
         racket/match
         struct-plus-plus)

(provide current-task
         task++  task
         task.id       task-id
         task.status   task-status
         task.data     task-data
         task?
         start-majordomo
         stop-majordomo
         majordomo?
         update-data
         keepalive
         success
         failure
         add-task)

;;----------------------------------------------------------------------

(struct++ majordomo
          ([(id    (gensym "majordomo-"))  symbol?] ; makes it human-identifiable
           [(cust  (make-custodian))       custodian?]))

(struct++ task
          ([(id (gensym "task-"))       symbol?]
           [(status 'unspecified)       (or/c 'success 'failure 'unspecified 'timeout)]
           [(data (hash))               any/c]
           ; private fields
           [(manager-ch (make-channel)) channel?]))

(define/contract current-task
  (parameter/c task?)
  (make-parameter #f #f 'current-task))

;;----------------------------------------------------------------------

(define (start-majordomo)
  (majordomo++))

;;----------------------------------------------------------------------

(define/contract (stop-majordomo jarvis)
  (-> majordomo? any)
  (custodian-shutdown-all (majordomo.cust jarvis)))

;;----------------------------------------------------------------------

(define finalized? (make-parameter #f))

(define (update-data data)
  (define the-task (current-task))
  (current-task (set-task-data the-task data))
  (channel-put (task.manager-ch the-task) (list 'update-data data)))

(define (keepalive) (channel-put (task.manager-ch (current-task)) 'keepalive))
(define (success [data the-unsupplied-arg])
  (finalized? #t)
  (finish-with 'success
               (if (unsupplied-arg? data)
                   (current-task)
                   (set-task-data (current-task) data))))

(define (failure [data the-unsupplied-arg])
  (finalized? #t)
  (finish-with 'failure
               (if (unsupplied-arg? data)
                   (current-task)
                   (set-task-data (current-task) data))))

(define/contract (finish-with status the-task)
  (-> symbol? task? any)
  (channel-put (task.manager-ch the-task)
               (list status the-task)))

;;----------------------------------------------------------------------

(define (finalize the-task result-ch
                  #:sort-op           sort-op
                  #:sort-key          sort-key
                  #:sort-cache-keys?  cache-keys?
                  #:pre               pre
                  #:post              post)
  (define raw-data (task.data the-task))

  (channel-put result-ch
               (set-task-data the-task
                              (post
                               (let ([data (pre raw-data)])
                                 (cond [sort-op (sort data          sort-op
                                                      #:key         sort-key
                                                      #:cache-keys? cache-keys?)]
                                       [else    data]))))))

;;----------------------------------------------------------------------

(define/contract (add-task jarvis action
                           #:keepalive         [keepalive  5]
                           #:retries           [retries    +inf.0]
                           #:parallel?         [parallel?  #f]
                           #:sort-op           [sort-op    #f]
                           #:sort-key          [sort-key   identity]
                           #:sort-cache-keys?  [cache-keys? #f]
                           #:pre               [pre        identity]
                           #:post              [post       identity]
                           . args)
  (->* (majordomo? (unconstrained-domain-> any/c))
       (#:keepalive         (and/c real? (not/c negative?))
        #:retries           (or/c exact-nonnegative-integer? +inf.0)
        #:parallel?         boolean?
        #:sort-op           (or/c #f (-> any/c any/c any/c))
        #:sort-key          (-> any/c any/c)
        #:sort-cache-keys?  boolean?
        #:pre               procedure?
        #:post              procedure?)
       #:rest list?
       channel?)

  (define result-ch (make-channel))
  (cond [parallel?
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

         ; Collect all the results from the subtasks
         (define raw-data (map (compose task.data sync) subtask-channels))

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
                                  #:keepalive         [keepalive  5]
                                  #:retries           [retries    +inf.0]
                                  #:parallel?         [parallel?  #f]
                                  #:sort-op           [sort-op    #f]
                                  #:sort-key          [sort-key   identity]
                                  #:sort-cache-keys?  [cache-keys? #f]
                                  #:pre               [pre        identity]
                                  #:post              [post       identity])
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
            (define result (apply action args))
            (when (not (finalized?))
              (success result)))))))

    (define worker (start-worker the-task))

    ; manager
    (thread
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
                     #:pre               pre
                     #:post              post)])))))
  result-ch)




;; ;; ; task
;; ;; ;   - id
;; ;; ;   - parallel?
;; ;; ;   - results-ch
;; ;; ;
;; ;; ; - start a task
;; ;; ; - make it easy to parallelize
;; ;; ;    - combine the results of parallel procs
;; ;; ; - be able to get the results back from the task
;; ;; ; - have the manager restart tasks if they fail
;; ;; ; - retain state across retries
;; ;; ; - shutting down the manager cleanly shuts down all tasks

;; ;; (define jarvis (start-majordomo))

;; ;; ; This is all you need.  It will run in a thread with a manager thread
;; ;; ; keeping an eye on the thread to restart it if it dies.
;; ;; (add-task jarvis check-filesystem)

;; ;; ; This will try up to 5 times to ping a collaborator
;; ;; (add-task jarvis ping '(bob) #:retries 5)

;; ;; ; This will download things in parallel.  There is no feedback about
;; ;; ; the results.
;; ;; (add-task jarvis download '("page1.html" "page2.html" "page3.html") #:parallel #t)

;; ;; ; This will return the results in no particular order
;; ;; (define task (add-task jarvis get-user-profiles '(alice bob fred) #:parallel #t))
;; ;; (println (sync (task.results-ch task)))

;; ;; ; This will sort the results before returning them
;; ;; (define task (add-task jarvis get-user-profiles
;; ;;                        '(alice bob fred)
;; ;;                        #:parallel #t
;; ;;                        #:sort-op symbol<?
;; ;;                        #:sort-key (curryr hash-ref 'name)))
;; ;; (println (sync (task.results-ch task)))


;; ;; ; This will post-process the results before returning them
;; ;; (define task (add-task jarvis get-user-profiles
;; ;;                        '(alice bob fred)
;; ;;                        #:parallel #t
;; ;;                        #:post (λ (lst) (map (curryr hash-set 'processed (current-seconds))))))
;; ;; (println (sync (task.results-ch task)))
