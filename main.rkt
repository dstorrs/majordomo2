#lang racket/base

(require racket/require
         (multi-in racket (async-channel  contract/base  contract/region  function  list  match promise))
         queue
         struct-plus-plus
         thread-with-id
         try-catch)

(provide start-majordomo
         stop-majordomo
         majordomo?
         majordomo.id
         majordomo-id
         majordomo.max-workers
         majordomo-max-workers

         task++  task
         task.id       task-id
         task.status   task-status
         task.data     task-data
         task?
         task-status/c
         current-task
         current-task-data

         is-success?
         is-not-success?
         is-failure?
         is-timeout?
         is-unspecified-status?

         update-data
         keepalive
         success
         failure

         flatten-nested-tasks

         add-task
         task-return-value
         get-task-data)

(define-logger majordomo2)

;;----------------------------------------------------------------------


(struct++ majordomo
          ([(id    (gensym "majordomo-"))             symbol?] ; makes it human-identifiable
           [(cust  (make-custodian))                  custodian?]
           [(max-workers +inf.0)                      (or/c +inf.0 exact-positive-integer?)]
           [(num-tasks-running 0)                     natural-number/c]
           [(queued-tasks (make-queue))               queue?]
           [(worker-listener-ch (make-async-channel)) async-channel?]
           [(task-manager-thd #f)                     (or/c #f thread?)]
           )
          (#:omit-reflection)
          #:mutable)

; on startup, create a thread that monitors worker-listener-ch
; - workers signal on that channel when they stop/start and when new tasks are queued
; - on receiving start
;   - increment num-workers-running
; - on receiving stop
;   - decrement num-workers-running
; - on receiving new-task
;   - start-worker
;

; - add-task
;   - add (delay <thing>) to the queued-tasks
;   - signal new task on worker-listener-ch
;
; - check-workers
;  - compare num-tasks-running to max-workers
;  - when there is room for another task,
;    - start-worker
;    - call check-workers
;
; - start-worker
;  - pop the queue
;  - force the promise

(define task-status/c (or/c 'success 'failure 'unspecified 'timeout 'mixed))
(struct++ task
          ([(id     (gensym "task-"))   symbol?]
           [(status 'unspecified)       task-status/c]
           [(data   (hash))             any/c]
           ; private fields
           [(finalized? #f)             boolean?]
           [(manager-ch (make-channel)) channel?]))

(define/contract (is-success? t)     (-> task? boolean?) (equal? 'success (task.status t)))
(define is-not-success? (negate is-success?))

(define/contract (is-failure? t)     (-> task? boolean?) (equal? 'failure (task.status t)))
(define/contract (is-timeout? t)     (-> task? boolean?) (equal? 'timeout (task.status t)))
(define/contract (is-unspecified-status? t) (-> task? boolean?)
  (equal? 'unspecified (task.status t)))


(define/contract current-task
  (parameter/c (or/c #f task?))
  (make-parameter #f #f 'current-task))

;;----------------------------------------------------------------------

(define/contract (start-majordomo #:max-workers [max-workers +inf.0])
  (->* () (#:max-workers (or/c +inf.0 exact-positive-integer?)) majordomo?)
  (log-majordomo2-debug "~a: starting majordomo..." (thread-id))

  ; create jarvis
  (define jarvis (majordomo++ #:max-workers max-workers))

  ; tell jarvis to start managing workers
  (thread-with-id
   (thunk
    (define listener-ch (majordomo.worker-listener-ch jarvis))
    (let loop ()
      (match (sync listener-ch)
        [(list 'add task-promise)
         ; a new task is being submitted. add the promise to the queue, run a worker if we can
         (match-define (struct* majordomo ([queued-tasks      queued-tasks]
                                           [num-tasks-running num-tasks-running]
                                           [max-workers       max-workers]))
           jarvis)
         (log-majordomo2-debug "~a ~a in queue-manager thread, adding task promise.  number of currently-running tasks: ~a. max-workers: ~a" (thread-id) (current-inexact-milliseconds) num-tasks-running max-workers)

         (define new-tasks  (queue-add queued-tasks task-promise))
         (cond [(< num-tasks-running max-workers)
                (log-majordomo2-debug "~a ~a running the task" (thread-id) (current-inexact-milliseconds))
                (define-values (next-task-promise new-queue) (queue-remove new-tasks))
                (set-majordomo-queued-tasks! jarvis new-queue)
                (set-majordomo-num-tasks-running! jarvis (add1 num-tasks-running))
                (force next-task-promise)]
               [else
                (log-majordomo2-debug "~a ~a NOT running the task" (thread-id) (current-inexact-milliseconds))
                (set-majordomo-queued-tasks! jarvis new-tasks)])]
        ;
        [(list 'stop id)
         ; a worker has stopped.  there is now room for another, so we can pull from the
         ; queue if there are any waiting tasks
         (log-majordomo2-debug "~a task id ~a has stopped" (thread-id) id)

         (match-define (struct* majordomo ([queued-tasks      queued-tasks]
                                           [num-tasks-running num-tasks-running]
                                           [max-workers       max-workers]))
           jarvis)
         (set-majordomo-num-tasks-running! jarvis (sub1 num-tasks-running))
         (when (not (queue-empty? queued-tasks))
           (log-majordomo2-debug "~a starting next task..." (thread-id))
           (define-values (next-task-promise new-queue) (queue-remove (majordomo.queued-tasks jarvis)))
           (set-majordomo-queued-tasks! jarvis new-queue)
           (set-majordomo-num-tasks-running! jarvis num-tasks-running)
           (force next-task-promise))])
      (loop))))

  ; return jarvis
  jarvis)

;;----------------------------------------------------------------------

(define/contract (stop-majordomo jarvis)
  (-> majordomo? any)
  (log-majordomo2-debug "~a: stopping majordomo..." (thread-id))
  (set-majordomo-queued-tasks! jarvis (make-queue)) ; let go of the jobs queue to help the GC
  (custodian-shutdown-all (majordomo.cust jarvis)))

;;----------------------------------------------------------------------

(define (update-data data)
  (log-majordomo2-debug "~a ~a: update-data with ~v" (thread-id) (current-inexact-milliseconds) data)
  (define the-task (current-task))
  (current-task (set-task-data the-task data))
  (channel-put (task.manager-ch the-task) (list 'update-data data)))

(define (keepalive)
  (log-majordomo2-debug "~a: keepalive" (thread-id))
  (channel-put (task.manager-ch (current-task)) 'keepalive))

(define (success [data the-unsupplied-arg])
  (log-majordomo2-debug "~a ~a: success! data is: ~v" (thread-id) (current-inexact-milliseconds) data)
  (finish-with 'success
               (let* ([the-task (set-task-finalized? (current-task) #t)]
                      [the-task (if (unsupplied-arg? data)
                                    the-task
                                    (set-task-data the-task data))])
                 the-task)))

(define (failure [data the-unsupplied-arg])
  (log-majordomo2-debug "~a ~a: failure! data is: ~v" (thread-id) (current-inexact-milliseconds) data)
  (finish-with 'failure
               (let* ([the-task (set-task-finalized? (current-task) #t)]
                      [the-task (if (unsupplied-arg? data)
                                    the-task
                                    (set-task-data the-task data))])
                 the-task)))

(define/contract (finish-with status the-task)
  (-> symbol? task? any)
  (log-majordomo2-debug "~a ~a: finish-with status: ~v and data: ~v"
                        (thread-id) (current-inexact-milliseconds) status (task.data the-task))
  (channel-put (task.manager-ch the-task)
               (list status (set-task-status the-task status))))

;;----------------------------------------------------------------------

; Once an action has been completed, the manager will call finalize.  finalize is
; responsible for filtering, preprocessing, sorting, and postprocessing the data stored in
; (current-task) and then doing a channel-put on the result-ch arg in order to make the
; value available to the customer.
(define/contract (finalize the-task result-ch
                           #:sort-op               sort-op
                           #:sort-key              sort-key
                           #:sort-cache-keys?      cache-keys?
                           #:filter                filter-func
                           #:pre                   pre
                           #:post                  post
                           #:flatten-nested-tasks? flatten-nested-tasks?)
  (-> task? channel?
      #:filter                (or/c #f procedure?)
      #:pre                   procedure?
      #:sort-op               (or/c #f (-> any/c any/c any/c))
      #:sort-key              (-> any/c any/c)
      #:sort-cache-keys?      boolean?
      #:post                  procedure?
      #:flatten-nested-tasks? boolean?
      any)

  (log-majordomo2-debug "~a ~a: entering finalize for task id: ~a"
                        (thread-id) (current-inexact-milliseconds) (task.id the-task))
  (define raw-data
    (task.data (if flatten-nested-tasks?
                   (flatten-nested-tasks the-task)
                   the-task)))

  (log-majordomo2-debug "~a: raw data is: ~v" (thread-id) raw-data)
  (log-majordomo2-debug "~a: filter func is: ~a" (thread-id) filter-func)

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

     (log-majordomo2-debug "~a: putting results on result-ch. status is: ~a. filtered data is: ~v"
                           (thread-id) (task.status the-task) filtered-data)

     (channel-put result-ch
                  (set-task-data the-task
                                 (post
                                  (let ([data (pre filtered-data)])
                                    (cond [sort-op (sort data          sort-op
                                                         #:key         sort-key
                                                         #:cache-keys? cache-keys?)]
                                          [else        data]))))))))


;;----------------------------------------------------------------------

(define/contract (add-task jarvis action
                           #:keepalive             [keepalive             5]
                           #:retries               [retries               3]
                           #:parallel?             [parallel?             #f]
                           #:unwrap?               [unwrap?               #f]
                           #:flatten-nested-tasks? [flatten-nested-tasks? #f]
                           #:filter                [filter-func           #f]
                           #:pre                   [pre                   identity]
                           #:sort-op               [sort-op               #f]
                           #:sort-key              [sort-key              identity]
                           #:sort-cache-keys?      [cache-keys?           #f]
                           #:post                  [post                  identity]
                           .                       args)
  (->* (majordomo? (unconstrained-domain-> any/c))
       (#:keepalive             (and/c real? (not/c negative?))
        #:retries               (or/c natural-number/c +inf.0)
        #:parallel?             boolean?
        #:unwrap?               boolean?
        #:flatten-nested-tasks? boolean?
        #:filter                (or/c #f procedure?)
        #:pre                   procedure?
        #:sort-op               (or/c #f (-> any/c any/c any/c))
        #:sort-key              (-> any/c any/c)
        #:sort-cache-keys?      boolean?
        #:post                  procedure?)
       #:rest list?
       channel?)

  (log-majordomo2-debug "~a entering add-task for action ~v with args ~v"
                        (thread-id) action args)
  (define result-ch (make-channel))
  (cond [parallel?
         (log-majordomo2-debug "~a: add-task in parallel" (thread-id))

         ; Spawn each argument off into its own task which will run in its own thread.

         ;  NOTE: In some cases it will be easier for the customer to pass args as a list
         ;  instead of as separate arguments, for example when the args are generated via
         ;  a 'map'.  In that case they can use #:unwrap? #t to have us unwrap it for them.
         (define subtasks
           (map sync
                (for/list ([arg (in-list (if unwrap? (car args) args))])
                  (add-task-helper jarvis
                                   action
                                   (list arg)
                                   (make-channel)
                                   #:flatten-nested-tasks? flatten-nested-tasks?
                                   #:keepalive             keepalive
                                   #:retries               retries
                                   #:parallel?             #f))))

         ; Collect the statuses (stati? whatever) from the subtasks
         (define statuses (map task.status subtasks))
         (log-majordomo2-debug "raw statuses before dedup: ~v" statuses)

         (define final-status
           (match (remove-duplicates statuses)
             ['()                                (raise "no statuses?")]
             [(list 'success)                    'success]
             [(list 'failure)                    'failure]
             [(list-no-order 'success other ...) 'mixed]
             [(list-no-order 'mixed   other ...) 'mixed]
             [_                                  'failure]))

         ; Finalize them in another thread, since finalize uses channel-put, which is
         ; blocking.
         (thread-with-id (thunk (finalize (task++ #:data subtasks #:status final-status)
                                          result-ch
                                          #:flatten-nested-tasks? flatten-nested-tasks?
                                          #:sort-op               sort-op
                                          #:sort-key              sort-key
                                          #:sort-cache-keys?      cache-keys?
                                          #:filter                filter-func
                                          #:pre                   pre
                                          #:post                  post)))
         ; return the result channel
         result-ch]
        [else
         (log-majordomo2-debug "~a: add-task NOT in parallel" (thread-id))
         (add-task-helper jarvis
                          action
                          args
                          result-ch
                          #:flatten-nested-tasks? flatten-nested-tasks?
                          #:keepalive             keepalive
                          #:retries               retries
                          #:parallel?             parallel?
                          #:unwrap?               unwrap?
                          #:sort-op               sort-op
                          #:sort-key              sort-key
                          #:sort-cache-keys?      cache-keys?
                          #:filter                filter-func
                          #:pre                   pre
                          #:post                  post)]))

;;----------------------------------------------------------------------

(define/contract (add-task-helper jarvis action args result-ch
                                  #:flatten-nested-tasks? [flatten-nested-tasks? #f]
                                  #:keepalive             [keepalive    5]
                                  #:retries               [retries      3]
                                  #:parallel?             [parallel?    #f]
                                  #:unwrap?               [unwrap?      #f]
                                  #:filter                [filter-func  #f]
                                  #:pre                   [pre          identity]
                                  #:sort-op               [sort-op      #f]
                                  #:sort-key              [sort-key     identity]
                                  #:sort-cache-keys?      [cache-keys?  #f]
                                  #:post                  [post         identity])
  (->* (majordomo? (unconstrained-domain-> any/c) list? channel?)
       (#:keepalive             (and/c real? (not/c negative?))
        #:flatten-nested-tasks? boolean?
        #:retries               (or/c exact-nonnegative-integer? +inf.0)
        #:parallel?             boolean?
        #:unwrap?               boolean?
        #:sort-op               (or/c #f (-> any/c any/c any/c))
        #:sort-key              (-> any/c any/c)
        #:sort-cache-keys?      boolean?
        #:filter                (or/c #f procedure?)
        #:pre                   procedure?
        #:post                  procedure?)
       channel?)
  (define control-ch (majordomo.worker-listener-ch jarvis))

  (async-channel-put
   control-ch
   (list 'add
         (delay
           (parameterize ([current-custodian (majordomo.cust jarvis)]
                          [current-task      (task++)])
             (match-define (and the-task (struct* task ([manager-ch manager-ch])))
               (current-task))

             ; worker
             (define (start-worker the-task)
               (parameterize ([current-custodian (make-custodian)]
                              [current-task      the-task])
                 (thread-with-id
                  (thunk
                   (log-majordomo2-debug "~a Starting worker thread for:\n\t action: ~v\n\targs: ~v" (thread-id) action args)

                   ; The arguments are passed into a rest arg, meaning that they come to us as a
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
                   (with-handlers ([exn:break? raise]
                                   [any/c (λ (e)
                                            (log-majordomo2-debug "~a caught: ~v" (thread-id) e)
                                            (failure e))])
                     (define result (apply action (if unwrap? (car args) args)))
                     (log-majordomo2-debug "~a result was: ~v" (thread-id) result)
                     (when (not (task.finalized? (current-task)))
                       (success result)))))))

             (define worker (start-worker the-task))

             ; manager thread
             (thread-with-id
              (thunk
               (log-majordomo2-debug "~a Starting manager thread for action ~v" (thread-id) action)
               (define result
                 (defatalize
                   (let loop ([retries  retries]
                              [the-task the-task]
                              [worker   worker])
                     (log-majordomo2-debug "~a manager thread looping for action ~v, task id: ~a"
                                           (thread-id) action (task.id the-task))

                     ; set current-task, do not parameterize it.  We want it to stick around after loop exits.
                     (current-task the-task)
                     (match (sync/timeout keepalive manager-ch worker)
                       ['keepalive
                        (loop retries the-task worker)]
                       ;
                       [(list 'update-data data)
                        (loop retries (set-task-data the-task data) worker)]
                       ;
                       [(or #f (== worker))
                        #:when (> retries 0) ; timeout or thread died, can be retried
                        (log-majordomo2-debug "~a timeout or thread died for action ~v, can be retried" (thread-id) action)
                        (kill-thread worker)
                        (loop (sub1 retries)
                              the-task
                              (start-worker the-task))]
                       ;
                       [(or #f (== worker)) ; timeout or thread died, no retries left
                        (log-majordomo2-debug "~a timeout or thread died for action ~v, can NOT be retried" (thread-id) action)
                        (kill-thread worker)
                        (finalize (set-task-status the-task 'timeout) result-ch
                                  #:flatten-nested-tasks? flatten-nested-tasks?
                                  #:sort-op               sort-op
                                  #:sort-key              sort-key
                                  #:sort-cache-keys?      cache-keys?
                                  #:filter                filter-func
                                  #:pre                   pre
                                  #:post                  post)]
                       [(list status (? task? the-task))
                        (log-majordomo2-debug "~a task finished with status ~a" (thread-id) status)
                        (finalize (set-task-status the-task status) result-ch
                                  #:flatten-nested-tasks?  flatten-nested-tasks?
                                  #:sort-op                sort-op
                                  #:sort-key               sort-key
                                  #:sort-cache-keys?       cache-keys?
                                  #:filter                 filter-func
                                  #:pre                    pre
                                  #:post                   post)]))))

               (log-majordomo2-debug "~a finished manager thread for action ~v, notifying majordomo that it stopped. result was: ~v" (thread-id) action (task.id (current-task) result))

               ; notify majordomo that a worker has finished and it's okay to start another one
               (async-channel-put control-ch (list 'stop (task.id (current-task))))))))))
  result-ch)

;;----------------------------------------------------------------------

; This is here to provide a consistent interface in the case where you want a specific
; pre-known value to come back.  It will provide a channel which syncs to a task which
; contains the value in its task.data field and 'success in its status field.
;
; If the value given was a task then we'll assume that the user wants that task returned,
; not a task containing that task.  You might do that if you want the status field to be
; something other than 'success.
(define/contract (task-return-value val)
  (-> any/c channel?)

  (match val
    [(? task?)
     (define ch (make-channel))
     (thread-with-id (thunk (channel-put ch val)))
     ch]
    [_  (add-task (start-majordomo) identity val)]))

;;----------------------------------------------------------------------

(define (current-task-data)
  (task.data (current-task)))

;;----------------------------------------------------------------------

(define/contract (get-task-data jarvis action
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
       any/c)
  (log-majordomo2-debug "~a entering get-task-data for action ~v with args ~v"
                        (thread-id) action args)
  (task.data (sync (apply (curry add-task jarvis action
                                 #:keepalive         keepalive
                                 #:retries           retries
                                 #:parallel?         parallel?
                                 #:unwrap?           unwrap?
                                 #:filter            filter-func
                                 #:pre               pre
                                 #:sort-op           sort-op
                                 #:sort-key          sort-key
                                 #:sort-cache-keys?  cache-keys?
                                 #:post              post)
                          args))))

;;----------------------------------------------------------------------

; Sometimes we'll have a task that creates subtasks and we want to
; collapse all the data back together.
(define/contract (flatten-nested-tasks the-task)
  (->i ([the-task task?])
       ()
       [result task?]
       #:post (result the-task) (equal? (task.id result) (task.id the-task)))

  (define (helper val)
    (match val
      [(? task?) (helper (task.data val))]
      [(? list?) (map helper val)]
      [_         val]))

  (set-task-data the-task (helper (task.data the-task))))
