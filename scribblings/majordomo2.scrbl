#lang scribble/manual

@(require (for-label racket majordomo2)
          racket/sandbox
          scribble/example)

@title{majordomo2}

@author{David K. Storrs}

@defmodule[majordomo2]

@section{Introduction}

@racketmodname[majordomo2] is a task manager.  It obsoletes the original @racketmodname[majordomo] package.

Major features include:

@itemlist[
          @item{Restart tasks that fail}
               @item{Carry state across restarts}
               @item{Run tasks in parallel on request}
               @item{Pre-process results }
               @item{Sort results after pre-processing}
               @item{Post-process results}
               ]

@section{Demonstration}

The provided functions and data types are defined in the @secref["API"] section, but here are examples of practical usage. 

@subsection{Simple Example}


@(define eval
   (call-with-trusted-sandbox-configuration
    (lambda ()
      (parameterize ([sandbox-output 'string]
                     [sandbox-error-output 'string]
                     [sandbox-memory-limit 50])
        (make-evaluator 'racket)))))

@examples[
          #:eval eval
          #:label #f

	  (require majordomo2)

          (define (build-email-text msg names)
            (for/list ([name names])
              (format "Dear ~a: ~a" name msg)))

          (define names '("fred" "barney" "betty" "wilma" "bam-bam"))

          (define jarvis (start-majordomo))

          (define result-channel
            (add-task jarvis build-email-text "hi there" names))

          (channel? result-channel)

          (define result  (sync result-channel))
          (task? result) ; #t
          (pretty-print (task.data result))
          ;    '("Dear fred: hi there"
          ;      "Dear barney: hi there"
          ;      "Dear betty: hi there"
          ;      "Dear wilma: hi there"
          ;      "Dear bam-bam: hi there")

          @#reader scribble/comment-reader
	  ; stop-majordomo shuts down all tasks that were added to that instance.  This
	  ; shuts down the instance custodian which will shut down the custodian for all
	  ; the tasks.
	  (stop-majordomo jarvis)
	  ]

@subsection{Sorting Task Results}

@examples[
          #:eval eval
          #:label #f

	  (require majordomo2)

          (define jarvis (start-majordomo))

          @#reader scribble/comment-reader
          (pretty-print
           ; Do the same thing as we did in the prior section, but get the results back
           ; sorted.
           (task.data (sync (add-task jarvis build-email-text "hi there" names
                                      #:sort-op string<?))))
          ; Result:
          ; '("Dear bam-bam: hi there"
          ;   "Dear barney: hi there"
          ;   "Dear betty: hi there"
          ;   "Dear fred: hi there"
          ;   "Dear wilma: hi there")

          @#reader scribble/comment-reader
          (define (mock-network-function . ip-addrs)
            ; In real life, this would (e.g.) connect to the specified IP address,
            ; send a message, and return whether or not it succeeded.  For purposes of
            ; this demonstration we'll have it return an arbitrary status.
            (for/list ([addr ip-addrs]
                       [status (in-cycle '(success failure timeout))])
              (list (task.id (current-task)) addr status)))


          @#reader scribble/comment-reader
          (pretty-print
           ; This shows the baseline return value from mock-network-function
           (task.data (sync (add-task jarvis mock-network-function
                                      "4.4.4.4" "8.8.8.8" "172.67.188.90" "104.21.48.235"))))
          ; Returns:
          ; '((task-77546 "4.4.4.4" success)
          ;   (task-77546 "8.8.8.8" failure)
          ;   (task-77546 "172.67.188.90" timeout)
          ;   (task-77546 "104.21.48.235" success))

          @#reader scribble/comment-reader
          (pretty-print
           ; Here we have a more elaborate situation where we sort the results using a
           ; key-extraction function and cache the functions.
	   ;
	   ; sort-cache-keys? is unnecessary here since the key is cheap to calculate,
	   ; but it's included for sake of demonstration.
           (task.data (sync (add-task jarvis mock-network-function
                                      "4.4.4.4" "8.8.8.8" "172.67.188.90" "104.21.48.235"
				      #:sort-op symbol<?
				      #:sort-key last
				      #:sort-cache-keys? #t))))
          ; Returns:
          ; '((task-77546 "4.4.4.4" success)
          ;   (task-77546 "8.8.8.8" failure)
          ;   (task-77546 "172.67.188.90" timeout)
          ;   (task-77546 "104.21.48.235" success))

	  (stop-majordomo jarvis)
	  ]

@subsection{Pre- and Post-Processing}

@examples[
          #:eval eval
          #:label #f

	  (require majordomo2)

          (define jarvis (start-majordomo))

          @#reader scribble/comment-reader
          (pretty-print
           ; This is contrived and overly fancy example, but it demonstrates the
           ; functionality.  We'll generate the strings, append some text to the end after
           ; they've been generated, sort the strings by their length, and then put it all
           ; into titlecase.
           (task.data (sync (add-task jarvis build-email-text "hi there" names
                                      #:sort-op <
                                      #:sort-key string-length
                                      #:pre  (curry map (curryr string-append ", my friend."))
                                      #:post (curry map string-titlecase)))))
          ; Result:
          ; '("Dear Fred: Hi There, My Friend."
          ;   "Dear Betty: Hi There, My Friend."
          ;   "Dear Wilma: Hi There, My Friend."
          ;   "Dear Barney: Hi There, My Friend."
          ;   "Dear Bam-Bam: Hi There, My Friend.")

	  (stop-majordomo jarvis)
	  ]


@subsection{Restarting}

@examples[
          #:eval eval
          #:label #f

	  (require majordomo2)

          (define jarvis (start-majordomo))

          (define (failing-func)
            (raise-arguments-error 'failing-func "Fake error"))

          @#reader scribble/comment-reader
	  (pretty-print
           ; If a function raises an error then the result will contain the value raised.
           ; We need to set #:retries to a finite number or it will restart indefinitely.
           (format "Data after failure was: ~v"
                   (task.data (sync (add-task jarvis failing-func #:retries 0)))))


          @#reader scribble/comment-reader
          (define (func-times-out . args)
            ; Sometimes an action will time out without explicitly throwing an error.  If
            ; so, it will be restarted again with all of its original arguments. (Assuming
            ; it has retries left.)  We can use the 'data' value in (current-task) to
            ; carry state across the restart.
            (define state (task.data (current-task)))
            (define data
              (if (hash-empty? state)
                  (begin (displayln "Initial start.  Hello!") args)
                  (begin (displayln "restarting...")          remaining)))
            (match data
              ['() (success)]
              [(list current remaining ...)
               (update-data (hash-set* state
                                       'remaining-args remaining
                                       'results (cons (add1 current)
                                                      (hash-ref state 'results '()))))
               (displayln (format "Processing ~a..." current))
               (displayln "Intentional timeout to demonstrate restart.")
               (sync never-evt)])
            (displayln "Goodbye."))


          @#reader scribble/comment-reader
          (let ([result (sync (add-task jarvis func-times-out 1 2 3 4 5 6
                                        #:keepalive 0.1 ; max time to complete/keepalive
                                        #:retries   3
                                        #:post (λ (h)
                                                 (hash-set h
                                                           'results (reverse (hash-ref h 'results '()))))
                                        ))])
            ; Be sure to use struct* in your match pattern instead of struct.  The task
            ; struct contains private fields that are not documented or provided.
            (match-define (struct* task ([status status] [data data])) result)
            (displayln (format "Final result was: ~v" data))
            (displayln (format "Final status was: ~v" status)))

          @#reader scribble/comment-reader
          (define (long-running-task)
            ; If a task is going to take a long time, it can periodically send a keepalive
            ; so that the manager knows not to restart it.
            (for ([i 10])
              (sleep 0.1)
              (keepalive))
            (success 'finished))

          (let ([result (sync (add-task jarvis long-running-task #:keepalive 0.25))])
            (displayln (format "Final status of long-running task was: ~v" (task.status result)))
            (displayln (format "Final data of long-running task was: ~v" (task.data result)))
            )


	  (stop-majordomo jarvis)
	  ]


@subsection[#:tag "running-in-parallel"]{Parallelized Example}

@examples[
          #:eval eval
          #:label #f

	  (require majordomo2)

          (define jarvis (start-majordomo))

          @#reader scribble/comment-reader
          (pretty-print
           ; Parallelize the task such that each argument in the list is farmed out to a
           ; separate sub task and the results are compiled back together. NB: Because of
           ; how mock-network-function is defined, this results in an extra layer of
           ; listing, as we're getting multiple lists each containing the results of
           ; processing a single argument instead of one list containing the results of
           ; processing each argument in turn.  See below for how to handle this.
           (task.data (sync (add-task jarvis mock-network-function
                                      #:parallel? #t
                                      "4.4.4.4" "8.8.8.8" "172.67.188.90" "104.21.48.235"))))

          @#reader scribble/comment-reader
          (pretty-print
           ; Same as above, but we'll append the sublists together in order to get back to
           ; the original results.
           (task.data (sync (add-task jarvis mock-network-function
                                      #:parallel? #t
                                      #:post (curry apply append)
                                      "4.4.4.4" "8.8.8.8" "172.67.188.90" "104.21.48.235"))))



	  (stop-majordomo jarvis)
          ]



@section[#:tag "API"]{API}

@defproc*[([(start-majordomo) majordomo?]
           [(stop-majordomo) void?])]{Start and stop a @racket[majordomo] instance and all tasks it manages.  An instance contains a @racket[custodian] which manages resources created by any tasks given to that instance.  These resources will be cleaned up when @racket[stop-majordomo] is called.}

@defproc[(majordomo.id [instance majordomo])  any/c]{Retrieve the unique ID for this particular instance.  It's possible to have as many majordomo instances as desired.  You might have multiple ones in order to group tasks together so that it's easy to shut down a specific group without disturbing others.}

@defproc[(majordomo? [val any/c]) boolean?]{Predicate for the @racket[majordomo] struct.}

@defproc[(task-status/c [val (or/c 'success 'failure 'unspecified 'timeout)]) boolean?]{Contract for legal task status values.}

@defproc[
         #:kind "task constructor"
         (task++ [#:id     id     symbol?       (gensym "task-")]
                 [#:status status task-status/c 'unspecified]
                 [#:data   data   any/c         (hash)])
         task?]{A task struct encapsulates the details of a task.  It has a unique ID as an aid in debugging and organization, a status to identify what the outcome of the task was, and the data generated by the execution of the task.

                  NOTE:  Although the base @racket[task] constructor is provided, you will not be able to use it since it contains private fields that are not documented.}

@defproc[(task? [the-task any/c]) boolean?]{Predicate for identifying tasks.}

@defproc*[([(task.id [the-task task?]) symbol?]
           [(task-id [the-task task?]) symbol?])
          ]{Accessors for the @racketid[id] field of a @racket[task] struct.}

@defproc*[([(task.status [the-task task?]) symbol?]
           [(task-status [the-task task?]) symbol?])
          ]{Accessors for the @racketid[status] field of a @racket[task] struct.}

@defproc*[([(task.data [the-task task?]) symbol?]
           [(task-data [the-task task?]) symbol?])
          ]{Accessors for the @racketid[data] field of a @racket[task] struct. See @secref["task-data"] for how the data field is used.}

@defproc[(set-task-id [the-task task?] [val symbol?]) task?]{Functional setter for the @racketid[id] field.}

@defproc[(set-task-status [the-task task?] [val symbol?]) task?]{Functional setter for the @racketid[status] field.}

@defproc[(set-task-data [the-task task?] [val symbol?]) task?]{Functional setter for the @racketid[data] field.  Prefer @racket[update-data] instead of using this directly, since that will also update the @racket[current-task] parameter and send a keepalive to the manager thread to delay restarting.}

@defparam[current-task t task? #:value #f]{A parameter that tracks the currently-running task.  It is functionally updated when you call the @racket[update-data], @racket[success], and @racket[failure] functions.}

@defproc[(keepalive) void?]{Notify the manager that the task is still running.  This resets the timer and prevents the manager from restarting the task.}

@defproc[(success [arg any/c the-unsupplied-arg]) void?]{If @racketid[arg] was supplied, set the @racketid[data] field of @racket[current-task] to @racketid[arg]. Set its @racketid[status] field to @racket['success].  Tell the manager that the worker has completed.  This will cause the manager to send the value of @racket[current-task] to the customer.}

@defproc[(failure [arg any/c the-unsupplied-arg]) void?]{If @racketid[arg] was supplied, set the @racketid[data] field of @racket[current-task] to @racketid[arg]. Set its @racketid[status] field to @racket['failure].  Tell the manager that the worker has completed.  This will cause the manager to send the value of @racket[current-task] to the customer.}

@defproc[(update-data [val any/c]) void?]{Set the @racketid[data] field of @racket[current-task] to @racketid[val]. Send a keepalive to the manager.}


@subsection[#:tag "task-data"]{Task Data}

The @racket[current-task] parameter holds a @racket[task] structure that can be used to carry state across restarts, such as which arguments have already been processed. (Since the task is started with the same argumens every time.)  The data field is also useful for returning a value from the action.  See @secref["task-results"] for details.

There are three functions that an action can call to manipulate the contents of the @racket[current-task] struct:

@itemlist[
          @item{@racket[(update-data val)] will both update the data field of the @racket[current-task] and send a keepalive to the manager indicating that the worker is still running.}
               @item{@racket[(success)] or @racket[(success arg)] will set its @racketid[status] field to @racket['success] and tell the manager that the worker has successfully completed.  If an argument was provided then the @racket[data] field will be updated to that value.}
               @item{@racket[(failure)] or @racket[(failure arg)] will set the task's @racketid[status] field to @racket['failure] and tell the manager that the worker has completed but failed.  If an argument was provided then the @racket[data] field will be updated to that value.}
               ]

If none of these functions is ever called then the data field will be set as follows:

@itemlist[
          @item{If the action completes without error, @racket[(success val)] will be called, where @racketid[val] is the return value of the action.}
               @item{If the action raises an error, @racket[(failure e)] will be called, where @racketid[e] is the raised value.}
               @item{If the action uses up all its retries and then times out, the data field is left undisturbed and the status is set to @racket['timeout].}
               ]

@subsection[#:tag "task-results"]{Task Results}

@racket[add-task] returns a channel.  When the task finishes, the content of the @racket[current-task] parameter is placed onto the channel.  The customer may retrieve the struct (via @racket[sync], @racket[channel-get], etc) and examine the @racketid[status] and @racketid[data] fields in order to determine how the task completed and what the final result was.

@section{Running tasks}

Tasks are created inside, and managed by, a @racket[majordomo] instance.

@defproc[(add-task [jarvis majordomo?]
                   [action (unconstrained-domain-> any/c)]
                   [arg                any/c] ...
                   [#:keepalive        keepalive-time   (and/c real? (not/c negative?)) 5]
                   [#:retries          retries          (or/c exact-nonnegative-integer? +inf.0) +inf.0]
                   [#:parallel?        parallel?        boolean? #f]
                   [#:sort-op          sort-op          (or/c #f (-> any/c any/c any/c)) #f]
                   [#:sort-key         sort-key         (-> any/c any/c) identity]
                   [#:sort-cache-keys? sort-cache-keys? boolean? #f]
                   [#:pre              pre              procedure? identity]
                   [#:post             post             procedure? identity]
                   )
         channel?]{
                   Add a task to a majordomo instance.

                       Two threads are created, a worker and a manager.  The worker does @racket[(apply action args)].  (See @secref["parallel-processing"] for an exception.) The manager does a @racket[sync/timeout keepalive-time] on the worker.  If the worker times out then the worker thread is killed and a new thread is started using the same arguments as the original and the most recent value of @racket[current-task], thereby allowing state to be carried over from one attempt to the next.  The keepalive timer resets whenever the worker does any of the following:

                       @itemlist[
                                 @item{Terminate (either naturally or by raising an error)}
                                      @item{Call @racket[keepalive]}
                                      @item{Call @racket[update-data]}
                                      @item{Call @racket[success]}
                                      @item{Call @racket[failure]}
                                      ]

                       Arguments are as follows:

                       @racketid[keepalive-time] The duration within which the worker must either terminate or notify the manager that it's still running.

                       @racketid[retries] The number of times that the manager should restart the worker before giving up.

                       @racketid[parallel?] Whether the action should be run in parallel. See @secref["parallel-processing"].

                       @racketid[pre] Pre-processes the results of the action immediately upon its completion.  The default preprocessor is @racket[identity].

                       @racketid[sort-op], @racketid[sort-key], @racketid[sort-cache-keys?] Whether and how to sort the results of the action.  They are passed as the (respectively) @racketid[less-than?], @racket[#:key extract-key] and @racket[#:cache-keys? cache-keys?] arguments to a @racket[sort] call.  Sorting happens after preprocessing and before postprocessing.

                       @racketid[post] Postprocesses the results of the action immediately before returning them.  The default is @racket[identity].

                       Obviously, if the results of your function are not a list, either leave @racketid[sort-op] as @racket[#f] so that it doesn't try to sort and fail, or else use @racket[#:pre list] to make it a list before sorting is applied.
                       }


@section[#:tag "parallel-processing"]{Parallel Processing}

As shown in the @secref["running-in-parallel"] demonstration, tasks can be parallelized simply by passing @racket[#:parallel? #t].  In this case @racket[add-task] will call itself recursively, creating subtasks and passing each of them one of the arguments in turn.  The main task will then wait for the subtasks to complete, aggregate their results into a list, and treat it as normal by running it through preprocessing, maybe sorting, and postprocessing.  The subtasks will be run with the same @racket[#:keepalive] and @racket[#:retries] values as the main tasks but everything else will be the default, meaning that all preprocessing and sorting will happen in the main task.

Caveats:

@itemlist[
          @item{If your function returns a list and you run it in parallel mode then you will end up with a list of one-element lists.  If this isn't what you want then you can use @racket[#:pre (curry apply append)] to resolve it back into a single list.}
               @item{Obviously, your subtasks are running in separate threads.  Their @racket[current-task] will not be the same as the one in the original task.}
               @item{Only the data from the subtasks is returned, meaning that the status values are unavailable to the final customer.  One solution to this is to include the status into the data field or to throw an exception.  Better solutions are solicited.}
               ]

@section{Notes}

The @racket[task] structure is defined via the @racketmodname[struct-plus-plus] module, giving it a keyword constructor, dotted accessors, reflection data, etc.  As stated above, not all accessors are exported, so if you need to use @racket[match] on a struct, use @racket[struct*] instead of @racket[struct].
