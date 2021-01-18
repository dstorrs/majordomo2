#lang racket/base

(require handy/test-more
         handy/try
         racket/function
         "../main.rkt")

(test-suite
 "majordomo"
 (is-type (start-majordomo) majordomo? "got expected type"))


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

#;
(test-suite
 "tasks can be automatically parallelized"

 (define result (sync (add-task (start-majordomo) add1 1 2 3 4 5
                                #:parallel? #t
                                #:sort-op <)))
 (is (task.data result)
     '(2 3 4 5 6)
     "Successfully parallelized")
 
 
 )
