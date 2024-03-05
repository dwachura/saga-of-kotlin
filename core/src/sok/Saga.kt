package io.dwsoft.sok

sealed interface Saga<Success, out Failure> {
    interface NonSuspending<Success, Failure> : Saga<Success, Failure> {
        /**
         * @throws SagaException
         */
        fun execute(): Result<Success, Failure>
        fun toReversibleOp(): ReversibleOp.NonSuspending<Result<Success, Failure>>
    }

    interface Suspending<Success, Failure> : Saga<Success, Failure> {
        /**
         * @throws SagaException
         */
        suspend fun execute(): Result<Success, Failure>
        fun toReversibleOp(): ReversibleOp.Suspending<Result<Success, Failure>>
    }

    sealed interface Result<out Success, out Failure> {
        @JvmInline
        value class Success<Success>(val value: Success) : Result<Success, Nothing>

        @JvmInline
        value class Failure<Failure>(val reason: Failure) : Result<Nothing, Failure>
    }
}

class SagaException private constructor(
    message: String? = null,
    cause: Throwable? = null,
) : RuntimeException(message, cause, true, true) {
    constructor(cause: Throwable) : this(message = null, cause = cause)
    constructor(message: String) : this(message = message, cause = null)
}
