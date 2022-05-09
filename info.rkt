#lang info

(define collection "majordomo2")
(define deps '("base" "queue" "struct-plus-plus" "thread-with-id" "in-out-logged"))
(define build-deps '("scribble-lib" "racket-doc" "rackunit-lib" "handy" "sandbox-lib"))
(define scribblings '(("scribblings/majordomo2.scrbl" ())))
(define pkg-desc "Managed job queue with stateful retry and automatic parallelization.  Obsoletes the original 'majordomo' package.")
(define version "1.1")
(define pkg-authors '("David K. Storrs"))

(define test-omit-paths '("test-main.rkt"))

