#lang racket

(require handy/test-more
         handy/try
         racket/require
         (multi-in racket (async-channel contract/base format function list match set))
         thread-with-id
         "test-lib.rkt"
         "../main.rkt"

         handy/utils
         racket/pretty
         racket/port)

(expect-n-tests 94)

(define-logger majordomo2-tests)

(test-suite
 "majordomo"
 (is-type (start-majordomo) majordomo? "got expected type"))


(test-suite
 "flatten-nested-tasks"

 ; tasks are not #:transparent so they will never be equal? unless they are eq?
 (define ch (make-channel))
 (define (t id data)
   (task++ #:id         id
           #:status     'success
           #:data       data
           #:manager-ch ch))

 (define (te? t1 t2)
   (and (equal? (task.id     t1) (task.id     t2))
        (equal? (task.data   t1) (task.data   t2))))

 ; If t1 data is (not/c list? task?) then t1 does not change
 (ok (te? (flatten-nested-tasks (t 'x 1))
          (t 'x 1))
     "if task data is neither task nor list, task is unchanged")

 ; If t1 data is (listof (not/c task?)) then t1 does not change
 (ok (te? (flatten-nested-tasks (t 'x '(a b)))
          (t 'x '(a b)))
     "if task data is (listof (not/c task?)), task is unchanged")

 ; If t1 data is task? then t1's data is set to inner task's data
 (ok (te? (flatten-nested-tasks (t 'outer (t 'inner 'x)))
          (t 'outer 'x))
     "where data is task?, lifts data from nested task")

 ; If t1 data is (list task?) then t1's data is set to (list inner-task-data)
 (ok (te? (flatten-nested-tasks (t 'outer (list (t 'inner 'x))))
          (t 'outer '(x)))
     "where data is (list task?), lifts data from nested task")

 ; Recursively applied to data that are lists of task/not-task
 (ok (te? (flatten-nested-tasks (t 'outer (list (t 'inner1 'x)
                                                'val1
                                                (t 'inner2 'y))))
          (t 'outer '(x val1 y)))
     "recurses across lists of tasks/not tasks where subtasks have not-task data")

 ; Recursively applied to all data regardless of type
 (ok (te? (flatten-nested-tasks (t 'outer (list (t 'inner1 (list (t 'nested1 'x)))
                                                'val1
                                                (t 'inner2 'y))))
          (t 'outer '((x) val1 y)))
     "recurses across lists of anything")

 (define (make-task x)
   (task++ #:data x))

 (let* ([jarvis (start-majordomo)]
        [without-flatten (sync (add-task jarvis make-task 0 1 2 #:parallel? #t))]
        [with-flatten    (sync (add-task jarvis make-task 0 1 2
                                         #:parallel? #t
                                         #:flatten-nested-tasks? #t))])
   (log-majordomo2-tests-debug "without flatten, results were: ~v" without-flatten)
   (is-type without-flatten task? "got a single task")
   (define data (task.data without-flatten))
   (ok (andmap task? data) "all data items were tasks, yay!")

   (define subdata (map task.data data))
   (log-majordomo2-tests-debug "subdata is ~v" subdata)
   (is (map task.data subdata)
       '(0 1 2)
       "without #:flatten-nested-tasks? we get three double-nested tasks, as expected")

   (is (task.data with-flatten)
       '(0 1 2)
       "with #:flatten-nested-tasks? we get a single-level task, as expected")

   (stop-majordomo jarvis))
 )


(test-suite
 "add-task"

 (define jarvis (start-majordomo))

 ;;----------------------------------------------------------------------

 ;; When a task ends we should get back a task struct.
 (define (basic) (success))
 (let ([result-ch (add-task jarvis basic)])
   (is-type result-ch channel? "add-task returns a channel")
   (define result (sync result-ch))
   (is-type result task? "result is a task")
   (is (task.data result) (hash) "(basic) correctly did not update the data"))

 ;;----------------------------------------------------------------------

 ;; If a task finishes without declaring its results, it shouldn't retry indefinitely
 (let ()
   (define (no-declaration) 'ok)
   (define result (sync (add-task jarvis no-declaration)))
   (is (task.data result) 'ok "functions that exit cleanly do not retry indefinitely and are considered successful"))

 ;;----------------------------------------------------------------------

 ;; If the task used (success v) then v should be the data on the task that is returned
 (let ()
   (define (succeed-at-add-up a b c)
     (success (+ a b c)))
   (define result (sync (add-task jarvis succeed-at-add-up 1 2 3)))
   (is (task.status result) 'success "add-to-6 gave correct status")
   (is (task.data   result) 6        "add-to-6 gave correct data"))

 ;; If the task used (failure v) then v should be the data on the task that is returned
 (let ()
   (define (fail-at-add-up a b c)
     (failure (+ a b c)))
   (define result (sync (add-task jarvis fail-at-add-up 1 2 3)))
   (is (task.status result) 'failure "fail-to-6 gave correct status")
   (is (task.data   result) 6        "fail-to-6 gave correct data"))

 ;;----------------------------------------------------------------------

 ;;  If a task throws, the value thrown becomes the data
 (for ([v (list 1 'oops "fail" (exn "oops" (current-continuation-marks)))]
       [type '(int sym str exn)])
   (is (task.data (sync (add-task jarvis (thunk (raise v)))))
       v
       (format "as expected, task threw a ~a" type)))

 ;;----------------------------------------------------------------------

 ;; We can update task data
 (let ()
   (define (with-data-updates)
     (define the-task (current-task))
     (is (task.data the-task) (hash) "default data is (hash)")
     (for ([i 3])
       (update-data i)
       (is (task.data (current-task)) i (format "after update, task.data was ~a" i)))
     (success (task.data (current-task))))

   (define result (sync (add-task jarvis with-data-updates)))
   (is (task.data result) 2 "with-data-updates got correct data")
   (is (task.status result) 'success "with-data-updates got correct status"))

 ;;----------------------------------------------------------------------

 ;; If the task times out then the data will be whatever the last thing we updated with
 ;; but the status will be 'timeout
 (let ()
   (define (timeout)
     (update-data 7)
     (sync never-evt))

   (define result (sync (add-task jarvis timeout #:keepalive 1 #:retries 3)))
   (is (task.status result) 'timeout "status on task was 'timeout")
   (is (task.data   result) 7 "after timeout, data on task was 7"))

 ;;----------------------------------------------------------------------

 ;; data can be sorted
 (let ()
   (define (basic-sortable)
     (update-data  '(1 7 3 5 2))
     (success))

   (is (task.data (sync (add-task jarvis basic-sortable #:sort-op <)))
       '(1 2 3 5 7)
       "data on task was sorted correctly")

   (is (task.data (sync (add-task jarvis basic-sortable #:sort-op < #:pre (curry map add1))))
       '(2 3 4 6 8)
       "data on task was pre-processed and sorted correctly")

   (is (task.data (sync (add-task jarvis basic-sortable
                                  #:pre     (curry map add1)
                                  #:sort-op <
                                  #:post    (curry apply +)
                                  )))
       (+ 2 3 4 6 8)
       "data on task was pre-processed, sorted, and post-processed correctly")

   (define (other-sortable)
     (success (list (hash 'a 11) (hash 'a 4) (hash 'a 7) (hash 'a 1))))
   (is (task.data (sync (add-task jarvis other-sortable
                                  #:sort-op          <
                                  #:sort-key         (curryr hash-ref 'a)
                                  #:sort-cache-keys? #t)))
       (list #hash((a . 1)) #hash((a . 4)) #hash((a . 7)) #hash((a . 11)))
       "data on task was sorted with keys and keys were cached"))

 )

(test-suite
 "tasks can be automatically parallelized; their status will be set appropriately"


 (let ([result (sync (add-task (start-majordomo)
                               add1
                               2 1 4 5 3 6
                               #:parallel? #t
                               #:sort-op <
                               #:sort-key task.data))])

   (ok (is-success? result) "parallelized task succeeded" )
   (define subtasks (task.data result))
   (ok (andmap task? subtasks) "as expected, data from a parallel run is subtasks")

   (is (map task.data subtasks)
       '(2 3 4 5 6 7)
       "sorting worked with a parallelized task"))


 (log-majordomo2-tests-debug "about to do the parallel raise test")
 (let* ([result (sync (add-task (start-majordomo)
                                (λ (arg) (raise-arguments-error 'oops "arg" arg))
                                1 2 3
                                #:parallel? #t))]
        [status           (task.status result)]
        [top-level-data   (task.data   result)])

   (log-majordomo2-tests-debug (format "parallel task had status: ~a" status))
   (log-majordomo2-tests-debug (format "parallel task had top level data: ~a" top-level-data))

   (is status 'failure "as expected, parallelized task is 'failure because all subtasks failed" )

   (ok (match top-level-data
         [(list (? task?) (? task?) (? task?)) #t]
         [_                                    #f])
       "as expected, data is three subtasks")
   (is (length top-level-data) 3 "got number of expected data items at top level")
   (ok (andmap task? top-level-data) "all data items were tasks, as expected")
   (ok (andmap (compose1 exn:fail? task.data) top-level-data)
       "as expected, all subtasks had exn:fail for data"))



 (log-majordomo2-tests-debug "HERE A. about to do mixed-status test with exns")

 (let ([result (sync (add-task (start-majordomo)
                               (λ (x)
                                 (when (even? x)
                                   (raise-arguments-error 'thingy "" x))
                                 x)
                               1 2 3 4 5
                               #:parallel?             #t
                               #:flatten-nested-tasks? #t))])
   (match-define (struct* task ([status status] [data data]))
     result)
   (log-majordomo2-tests-debug "HERE. status: ~a. data: ~v" status data)
   (is status 'mixed "parallel tasks where some succeed and some fail have status 'mixed")
   (ok (match data
         [(list-no-order 1 3 5 (? exn:fail?) (? exn:fail?)) #t]
         [_                                                 #f])
       "parallel tasks with mixed status return all the data unless told otherwise"))
 )

(test-suite
 "tasks can receive a list instead of separate args if desired"

 (define jarvis (start-majordomo))
 (try [
       (define (foo a b c)
         (+ a b c))

       (let ()
         (define result (task.data (sync (add-task jarvis foo 1 2 3))))
         (is result 6 "(add-task jarvis foo 1 2 3) works"))


       (let ()
         (define result (task.data (sync (add-task jarvis foo '(1 2 3)))))
         (is-type result exn:fail:contract? "(foo) dies when you pass it a list instead of separate args"))

       (let ()
         (define result (task.data (sync (add-task jarvis foo '(1 2 3) #:unwrap? #t))))
         (is result 6 "(foo) works when you pass it a list instead of separate args but you use #:unwrap? #t")
         )
       ]
      [finally (stop-majordomo jarvis)])
 )

(test-suite
 "results can be filtered before processing"

 (try [
       (define (simple)
         (update-data  '(1 2 3 4 5 6 #f "FAILED"))
         (success))

       ;  Filtering the results happens before pre-processing
       (let  ([result (sync (add-task (start-majordomo)
                                      simple
                                      #:filter  (and/c number? even?)
                                      #:retries 0
                                      #:pre     (λ (lst)
                                                  (if (memf odd? lst)
                                                      (raise 'failed)
                                                      lst))
                                      #:sort-op >))])
         (is (task.data result)
             '(6 4 2)
             "Successfully filtered results"))

       (define (result-null)
         (update-data  '())
         (success))
       (let  ([result (sync (add-task (start-majordomo)
                                      result-null
                                      #:filter  (and/c number? even?)
                                      #:retries 0
                                      #:pre     (λ (lst)
                                                  (if (memf odd? lst)
                                                      (raise 'failed)
                                                      lst))
                                      #:sort-op >))])
         (is (task.data result)
             '()
             "Successfully filtered a null list"))

       (define (result-false)
         (update-data  #f)
         (success))
       (let ([result (sync (add-task (start-majordomo) result-false #:retries 0))])
         (ok #t "if you don't specify a filter it's fine if data is not a list"))

       (let ([ch (add-task (start-majordomo)
                           result-false
                           #:filter  (and/c number? even?)
                           #:retries 0)])
         (define result (sync ch))
         (is-type (task.data result)
                  exn:fail:contract?
                  "fails if you try to filter something that isn't a list"))
       ]
      [catch (any/c (λ (e)
                      (display "failed to filter. Result:") (print e)
                      (ok #f "failed!  did not manage to filter results before preprocess")))]))

(test-suite
 "task-return-value"

 (for ([val (list 7 "foobar" (task++ #:status 'failure #:data 'oops))]
       [correct-status '(success success failure)]
       [correct-data  (list 7 "foobar" 'oops)])
   (let ([ch  (task-return-value val)])
     (is-type ch
              channel?
              (format "(task-return-value ~v) returns a channel, as expected"
                      val))
     (define result (sync ch))
     (is-type result task? "channel contains a task, as expected")
     (is (task.status result) correct-status "the task had the expected status")
     (is (task.data   result) correct-data   "the task had the expected data"))))

(test-suite
 "current-task-data"
 (parameterize ([current-task (task++ #:data 'outer)])
   (is (current-task-data) 'outer "outer param 1: (current-task-data) returns 'outer as expected")
   (parameterize ([current-task (task++ #:data 'inner)])
     (is (current-task-data) 'inner "inner param: (current-task-data) returns 'inner as expected"))
   (is (current-task-data) 'outer "outer param 2: (current-task-data) returns 'outer as expected")))

(test-suite
 "get-task-result"

 (define jarvis (start-majordomo))
 (is (get-task-data jarvis add1 7)
     8
     "get-task-result returns the value without having to explicitly sync and task.data")

 (stop-majordomo jarvis)
 )

(test-suite
 "status checks"

 (define s (task++ #:status 'success))
 (define f (task++ #:status 'failure))
 (define t (task++ #:status 'timeout))
 (define u (task++ #:status 'unspecified))

 (ok (and
      (is-success? s)
      (for/and ([func (list is-not-success? is-failure? is-timeout? is-unspecified-status?)])
        (not (func s))))
     "is-success? worked")

 (ok (and (is-failure? f)
          (is-not-success? f)
          (for/and ([func (list is-success? is-timeout? is-unspecified-status?)])
            (not (func f))))
     "is-failure? worked")

 (ok (and (is-timeout? t)
          (is-not-success? t)
          (for/and ([func (list is-success? is-failure? is-unspecified-status?)])
            (not (func t))))
     "is-timeout? worked")

 (ok (and (is-unspecified-status? u)
          (is-not-success? u)
          (for/and ([func (list is-success? is-failure? is-timeout?)])
            (not (func u))))
     "is-unspecified-status? worked")
 )

(test-suite
 "task limits"

 (define (run id ch)
   (let loop ([times-left 20])
     (async-channel-put ch (list id 'running))
     (when (> times-left 0)
       (sleep 50/1000)
       (keepalive)
       (loop (sub1 times-left))))
   (async-channel-put ch (list id 'done)))

 (define (max-running jarvis)
   (define ch (make-async-channel))

   (for ([i 10])
     (add-task jarvis run (format "job~a" i) ch))

   (let loop ([running (set)]
              [max-running 0])
     (match (sync ch)
       [(list id 'running)
        (define new-running (set-add running id))
        (define count (set-count new-running))
        (loop new-running (if (> count max-running) count max-running))]
       ;
       [(list id 'done)
        (define new-running (set-remove running id))
        (define count (set-count new-running))
        (if (zero? (set-count new-running))
            max-running
            (loop new-running (if (> count max-running) count max-running)))]
       )))

 (ok (> (max-running (start-majordomo)) 5) "by default, all tasks run more or less at once")
 ; >5 because maybe not 10 simultaneous tasks

 (is (max-running (start-majordomo #:max-workers 1)) 1 "#:max-workers 1 stayed limited to 1 task at a time")
 (is (max-running (start-majordomo #:max-workers 3)) 3 "#:max-workers 3 ran up to 3 tasks at a time")
 )

(test-suite
 "parameterization"
 (log-majordomo2-tests-debug "entering last test-suite")
 (define jarvis (start-majordomo))
 (define (fetch-param) (param))

 (is (param) 'original "direct call to (param) with default value works")
 (is (task.data (sync (add-task jarvis fetch-param))) 'original "baseline test of parameters via add-task")

 (parameterize ([param 'updated-once])
   (is (param) 'updated-once "direct call to (param) works inside parameterize")

   (is (task.data (sync (add-task jarvis fetch-param)))
       'updated-once
       "inside parameterized add-task, correctly returned 'updated-once")
   )

 (define ch (make-channel))

 (void
  (thread
   (thunk
    (say "in thread thunk, param is: " (param))
    (define p       (channel-get ch))
    (call-with-parameterization
     p
     (thunk
      (say "in call-with thunk, param is: " (param)))
     )
    )))

 (param 'updated-twice)

 (channel-put ch (current-parameterization))

 (log-majordomo2-tests-debug "about to do failing tests")
 (is (task.data (sync (add-task jarvis fetch-param)))
     'updated-twice
     "outside parameterize, after parameter assignment, correctly returned 'updated-twice")
 (is (task.data (sync (add-task jarvis fetch-param #:parameterization (current-parameterization))))
     'updated-twice
     "outside parameterize, after parameter assignment, using #:parameterization, correctly returned 'updated-twice")

 (define (message-all-users)
   (for ([u (users)])
     (say (thread-id) ", sending a message to: " u))
   (say (thread-id) " done.")
   )

 (define users (make-parameter '()))
 (define original-params (current-parameterization))

 ;; (say "should be null")
 ;; (void (sync (add-task jarvis message-all-users)))

 ;; (parameterize ([users '(alice bob charlie)])
 ;;   (say "should be a b c")
 ;;   (void (sync (add-task jarvis message-all-users))))

 (define inf-jarvis (start-majordomo))
 (define limited-jarvis (start-majordomo #:max-workers 5))

 (parameterize ([users '(alice bob charlie dan)])
   ; a new user was created. assume this is coming in from the DB or net
   (say "inf jarvis should print 'oops'")
   (users '(oops))
   (void (sync (add-task inf-jarvis message-all-users))))

 (parameterize ([users '(alice bob charlie dan)])
   ; a new user was created. assume this is coming in from the DB or net
   (say "limited jarvis should NOT print 'oops'")
   (users '(oops))
   (void (sync (add-task limited-jarvis message-all-users))))


 (parameterize ([users '(alice bob charlie dan emily)])
   ; a new user was created. assume this is coming in from the DB or net
   (say "should be null")
   (void (sync (add-task jarvis message-all-users #:parameterization original-params))))
 )

(test-suite
 "with-singleton-id"

 (log-majordomo2-tests-debug "entering with-singleton-id tests")
 (define run-forever (λ (n)
                       (log-majordomo2-tests-debug "in run-forever, n is: ~a " n)
                       (sleep 10)
                       (log-majordomo2-tests-debug "leaving run-forever, n is: ~a " n)))


 (let* ([jarvis-unlimited (start-majordomo)])
   (throws (thunk (add-task jarvis-unlimited
                            run-forever
                            1
                            #:with-singleton-id 'foo))
           #rx"majordomo instances without a limit on workers do not use the worker queue, so #:with-singleton-id is meaningless"
           "can't use #:with-singleton-id when you have unlimited workers")
   (stop-majordomo jarvis-unlimited))


 (let* ([jarvis-limited (start-majordomo #:max-workers 1)]
        [ok  (say "got jarvis")]
        [ignore
         (and (say "about to add first task")
              (add-task jarvis-limited
                        run-forever
                        2
                        #:retries 0
                        #:with-singleton-id 'foo))]
        [ch
         (and (say "about to add second task")
              (add-task jarvis-limited
                        run-forever
                        3
                        #:with-singleton-id 'foo))])
   (say "after adding tasks")
   (define the-task (sync ch))
   (say "got the task")

   (is (task.status the-task) 'rejected "task was rejected")
   (is (task.data the-task) "a task with this id is already running" "task data was as expected")
   (is (task.id the-task) 'foo "task data was as expected")
   (stop-majordomo jarvis-limited))

 )
