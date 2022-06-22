#lang racket/base

(require racket/require
         (multi-in racket (async-channel  contract/base  contract/region  function  list  match set))
         queue
         in-out-logged
         struct-plus-plus
         thread-with-id
         try-catch)

; only used for debugging
(require racket/pretty
         racket/port
         )



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
          ([(id    (gensym "majordomo-"))                 symbol?] ; makes it human-identifiable
           [(cust  (make-custodian))                      custodian?]
           [(max-workers +inf.0)                          (or/c +inf.0 exact-positive-integer?)]
           [(num-tasks-running 0)                         natural-number/c]
           [(queued-tasks (make-queue))                   queue?]
           [(running-task-ids (set))                      set?]
           [(worker-listener-ch (make-async-channel))     async-channel?]
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
;   - add (thunk <thing>) to the queued-tasks
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
;  - execute the thunk

(define task-status/c (or/c 'success 'failure 'unspecified 'timeout 'mixed 'rejected))
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
(define/contract (is-rejected? t)    (-> task? boolean?) (equal? 'rejected (task.status t)))
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
    (define (run-task jarvis)
      (match-define (struct* majordomo ([queued-tasks      queued-tasks]
                                        [running-task-ids  running-task-ids]
                                        [num-tasks-running num-tasks-running]
                                        [max-workers       max-workers]))
        jarvis)

      (define-values (next-task-info new-queue) (queue-remove queued-tasks))
      (log-majordomo2-debug "~a next-task-info: ~v. new queue after queue-remove is: ~v"
                            (thread-id)
                            next-task-info
                            (with-output-to-string
                              (thunk (pretty-print new-queue))))
      (match-define (list  next-task-id params next-task-thunk)
        next-task-info)


      (define new-running-ids  (set-add running-task-ids next-task-id))
      (log-majordomo2-debug "~a new running task ids: ~v" (thread-id) new-running-ids)

      (set-majordomo-queued-tasks!      jarvis new-queue)
      (set-majordomo-running-task-ids!  jarvis new-running-ids)
      (set-majordomo-num-tasks-running! jarvis (add1 num-tasks-running))
      (call-with-parameterization params
                                  next-task-thunk))

    (define listener-ch (majordomo.worker-listener-ch jarvis))
    (let loop ()
      (match (sync listener-ch)
        [(list 'add result-ch (and task-info (list the-task-id _ _)))
         ; originally used a task-info struct for most of that.  Then Racket got pissy
         ; about parsing it and I couldn't be arsed to figure it out --DKS

         (match-define (struct* majordomo ([queued-tasks      queued-tasks]
                                           [running-task-ids  running-task-ids]
                                           [num-tasks-running num-tasks-running]
                                           [max-workers       max-workers]))
           jarvis)

         (cond [(set-member? running-task-ids the-task-id)
                (log-majordomo2-error "~a task rejected because this id was already running: ~v"
                                      (thread-id) the-task-id)
                (parameterize ([current-task (task++ #:id the-task-id #:status 'rejected
                                                     #:data "a task with this id is already running")])
                  (thread-with-id
                   (thunk
                    (log-majordomo2-debug "~a calling finalize on task rejected because singleton ~v"
                                          (thread-id) the-task-id)
                    (finalize (current-task) result-ch
                              #:filter                #f
                              #:pre                   identity
                              #:sort-op               #f
                              #:sort-key              identity
                              #:sort-cache-keys?      #f
                              #:post                  identity
                              #:flatten-nested-tasks? #f))))]
               [else
                (log-majordomo2-debug "~a task id ~a was not found in the set" (thread-id) the-task-id)

                (define new-tasks  (queue-add queued-tasks task-info))
                (set-majordomo-queued-tasks! jarvis new-tasks)

                (log-majordomo2-debug "~a new queue after queue-add is: ~v"
                                      (thread-id)
                                      (with-output-to-string
                                        (thunk (pretty-print new-tasks))))

                (cond [(< num-tasks-running max-workers)
                       (run-task jarvis)]
                      [else
                       (log-majordomo2-debug "~a ~a NOT running the task" (thread-id) (current-inexact-milliseconds))])])]
        ;
        [(list 'stop id)
         ; a worker has stopped.  there is now room for another, so we can pull from the
         ; queue if there are any waiting tasks
         (log-majordomo2-debug "~a task id ~a has stopped" (thread-id) id)

         (match-define (struct* majordomo ([queued-tasks      queued-tasks]
                                           [running-task-ids  running-task-ids]
                                           [num-tasks-running num-tasks-running]
                                           [max-workers       max-workers]))
           jarvis)

         (define new-task-ids (set-remove running-task-ids id))
         (set-majordomo-running-task-ids jarvis new-task-ids)
         (set-majordomo-num-tasks-running! jarvis (sub1 num-tasks-running))

         (when (not (queue-empty? queued-tasks))
           (run-task jarvis))])
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
                           #:parameterization      [params                (current-parameterization)]
                           #:with-singleton-id     [singleton-task-id     #f]
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
        #:post                  procedure?
        #:parameterization      (or/c #f parameterization?)
        #:with-singleton-id     symbol?
        )
       #:rest list?
       channel?)
  (in/out-logged
   ("add-task" #:to majordomo2-logger
               "(thread-id)" (thread-id) "action" action "args" args "singleton-task-id" singleton-task-id
               "max-workers "(majordomo.max-workers jarvis))

   ; don't use a task singleton id if you're not using the task queue
   (when (and singleton-task-id (= +inf.0 (majordomo.max-workers jarvis)))
     (raise-arguments-error 'add-task
                            "majordomo instances without a limit on workers do not use the worker queue, so #:with-singleton-id is meaningless"))

   (log-majordomo2-debug "did not die from task/workers check")

   ;; majordomo has a max-worker field that defines the maximum number of tasks it can be
   ;; running at a time.  By default this is +inf.0, meaning no limit.  Otherwise, it will
   ;; put tasks in a queue and run them when a worker is available.
   ;;
   ;;  IMPORTANT: THERE'S A TRICKY PROBLEM HERE WHEN USING THE WORKER QUEUE.  It's not a
   ;;  bug, it's caused by how Racket manages parameters (meaning make-parameter, not
   ;;  function arguments).  The details are complicated and you can read more about them here:
   ;;
   ;;      https://racket.discourse.group/t/running-a-procedure-from-delay-does-not-capture-modified-parameters/936/21?u=dstorrs
   ;;
   ;; When this majordomo instance has:
   ;;
   ;;    UNlimited workers (+inf.0): it will go ahead and run the task immediately, without
   ;;    using the queue, and you can ignore the rest of this comment block.
   ;;
   ;;    limited workers: this function is going to do some futzing and then put a task
   ;;    into the worker queue to be run as soon as there's a worker available.
   ;;
   ;; Again, if you have unlimited workers, you can ignore the rest of this.
   ;;
   ;; You can also ignore the rest of this if your code does not do direct assignment to
   ;; parameters, such as (username "alice"), and instead either leaves them alone or does
   ;; (parameterize ([username "alice"]) ...)
   ;;
   ;;
   ;;
   ;;  The upshot is that changes to parameters are not visible in other threads (that's
   ;;  the point of parameters).  Since the worker queue is managed in a separate thread,
   ;;  your code may end up seeing a different value for a parameter when its called than
   ;;  when it was created.
   ;;
   ;;  majordomo tries to avoid this by passing in the appropriate parameterization with
   ;;  the task, but that will only capture changes made by way of (parameterize ([username
   ;;  "alice"]) ...) and not changes made by direct assignment such as (username "alice")
   ;;
   ;;  Example:
   ;;
   ;;   (define users (make-parameter '(alice))) ; parameter exists and has value '(alice) in all threads
   ;;   ; Create the instance and start the queue-management thread.  That thread gets a
   ;;   ; copy of the current parameterization.
   ;;   (define jarvis-unlimited (start-majordomo))  ; queue thread exists but will never be used
   ;;   (define jarvis-limited   (start-majordomo #:max-workers 5))  ; queue thread sees (users '(alice))
   ;;
   ;;   (users '(alice bob)) ; original thread sees the addition of bob, jarvis-limited's queue does not
   ;;
   ;;   (define (show-users n)
   ;;     (display n) (display ": ")
   ;;     (for ([user (users)])
   ;;       (display user))
   ;;     (displayln ""))
   ;;
   ;;   (define run (compose1 void sync)) ; run the task, wait for completion, discard result
   ;;   (show-users 0) ; => alicebob, reflecting the value of `users` in the main thread
   ;;   (run (add-task jarvis-unlimited show-users 1)) ; => 1: alicebob, the value in the main thread
   ;;   (run (add-task jarvis-limited show-users 2))   ; => 2: alice, the value in the queue thread
   ;;
   ;;     ; You may pass in any parametereterization you like.  Let's make one.
   ;;   (define ch (make-async-channel))
   ;;   (void (thread (thunk (parameterize ([users '(alice bob charlie dan)])
   ;;                          (async-channel-put ch (current-parameterization))))))
   ;;   (define params  (async-channel-get ch)) ; different from main thread and queue thread
   ;;
   ;;   (run (add-task #:parameterization params
   ;;                  jarvis-unlimited show-users 3)) ; => 3: alicebobcharliedan, the value in the sub thread
   ;;
   ;;  The queue thread in jarvis-limited was created when jarvis-limited was created.  Problem: When
   ;;  threads are created they get a copy of the then-current parameterization (the values
   ;;  of all parameters defined in the program).  The parameterization is thread-local
   ;;  meaning that changes made after the thread is created are not visible to the thread.
   ;;  Therefore, if we create the queue management thread and later modify parameters that
   ;;  the task will depend on, that task will see the original value of the parameter
   ;;  which is almost certainly not what you want.
   ;;
   ;;  To get around this, we pass in the parameterization.  This will fix the issue IF AND
   ;;  ONLY IF the creator of the code always used `parameterize` to update their
   ;;  parameters instead of direct assignment.  Again, see the Discourse thread for full
   ;;  details because this stuff be complicated yo.
   ;;
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
                                    #:parameterization      params
                                    #:with-singleton-id     singleton-task-id
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
                           #:parameterization      params
                           #:with-singleton-id     singleton-task-id
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
                           #:post                  post)])))

;;----------------------------------------------------------------------

(define/contract (add-task-helper jarvis action args result-ch
                                  #:parameterization      params
                                  #:with-singleton-id     [singleton-task-id     #f]
                                  #:flatten-nested-tasks? [flatten-nested-tasks? #f]
                                  #:keepalive             [keepalive             5]
                                  #:retries               [retries               3]
                                  #:parallel?             [parallel?             #f]
                                  #:unwrap?               [unwrap?               #f]
                                  #:filter                [filter-func           #f]
                                  #:pre                   [pre                   identity]
                                  #:sort-op               [sort-op               #f]
                                  #:sort-key              [sort-key              identity]
                                  #:sort-cache-keys?      [cache-keys?           #f]
                                  #:post                  [post                  identity]
                                  )
  (->* (majordomo? (unconstrained-domain-> any/c) list? channel?
                   #:parameterization      parameterization?)
       (
        #:with-singleton-id     (or/c #f symbol?)
        #:keepalive             (and/c real? (not/c negative?))
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
  (define new-task
    (if singleton-task-id
        (task++ #:id singleton-task-id)
        (task++)))

  (define task-thunk
    (parameterize ([current-custodian (majordomo.cust jarvis)]
                   [current-task      new-task])
      (thunk
       (log-majordomo2-debug "~a entering task thunk" (thread-id))
       (match-define (and the-task (struct* task ([manager-ch manager-ch])))
         new-task)

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
         (define tid (thread-id))
         (in/out-logged
          ("manager thread" #:to majordomo2-logger #:results (result)
                            "thread-id" tid "action" action "args (before unwrap)" args)
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

         ; notify majordomo that a worker has finished and it's okay to start another one
         (async-channel-put control-ch (list 'stop (task.id (current-task)))))))))


  (cond [(equal? +inf.0 (majordomo.max-workers jarvis))
         (call-with-parameterization
          (or params (current-parameterization)) ; they can pass #f to mean 'use current one'
          task-thunk)]
        [else
         (async-channel-put
          control-ch
          (list 'add
                result-ch
                (list (task.id new-task)
                      params
                      task-thunk)))])
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
