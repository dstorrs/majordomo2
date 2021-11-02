#lang racket/base

(require handy/test-more
         handy/try
         racket/contract/base
         racket/function
         racket/list
         "../main.rkt")

(expect-n-tests 67)

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

 ; If t1 data is (not/c list? task?) then t1 does not does not change
 (ok (te? (flatten-nested-tasks (t 'x 1))
          (t 'x 1))
     "if task data is neither task nor list, task is unchanged")

 ; If t1 data is (listof (not/c task?)) then t1 does not does not change
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
   (is (map task.data (task.data without-flatten))
       '(0 1 2)
       "without #:flatten-nested-tasks? we get a nested task, as expected")

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
   (is-type result task? "result is a task")
   (is (task.data result) 'ok "functions that exit cleanly do not retry indefinitely"))

 ;;----------------------------------------------------------------------

 ;; If the task used (success v) then v should be the data on the task that is returned
 (let ()
   (define (add-to-6 a b c)
     (success (+ a b c)))
   (define result (sync (add-task jarvis add-to-6 1 2 3)))
   (is (task.data result) 6 "add-to-6 gave correct data")
   (is (task.status result) 'success "add-to-6 gave correct status"))

 ;; If the task used (failure v) then v should be the data on the task that is returned
 (let ()
   (define (fail-to-6 a b c)
     (failure (+ a b c)))
   (define result (sync (add-task jarvis fail-to-6 1 2 3)))
   (is (task.data result) 6 "fail-to-6 gave correct data")
   (is (task.status result) 'failure "fail-to-6 gave correct status"))

 ;;----------------------------------------------------------------------

 ;;  If a task throws, the value thrown becomes the data
 (define ((throw-thing val)) (raise val))
 (for ([v (list 1 'oops "fail" (exn "oops" (current-continuation-marks)))]
       [type '(int sym str exn)])
   (is (task.data (sync (add-task jarvis (throw-thing v))))
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
 "tasks can be automatically parallelized"

 (define result (sync (add-task (start-majordomo)
                                add1
                                1 2 3 4 5
                                #:parallel? #t
                                #:sort-op <)))
 (is (task.data result)
     '(2 3 4 5 6)
     "Successfully parallelized"))

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
       (let ()
         (is (task.data (sync (add-task jarvis + 1 2 3 #:parallel? #t)))
             '(1 2 3)
             "(add-task jarvis + 1 2 3 #:parallel? #t) returns '(1 2 3")

         (define args '(1 2 3))
         (is-type (first (task.data (sync (add-task jarvis + args #:parallel? #t))))
                  exn:fail:contract?
                  "(add-task jarvis + '(1 2 3) #:parallel? #t) blows up, as expected")

         (is (task.data (sync (add-task jarvis + args #:parallel? #t #:unwrap? #t)))
             '(1 2 3)
             "(add-task jarvis + '(1 2 3) #:parallel? #t #:unwrap? #t) correctly returns '(1 2 3)"))
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

       (is-type (task.data (sync (add-task (start-majordomo)
                                           result-false
                                           #:filter  (and/c number? even?)
                                           #:retries 0)))
                exn:fail:contract?
                "fails if you try to filter something that isn't a list")

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
