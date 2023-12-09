package io.dwsoft.sok

sealed interface SagaPhase

/**
 * Revertible operation producing result of type T, composed of different revertible phases.
 */
class Saga<T>(val phases: List<SagaPhase>, val finalizer: Action<T>) : SagaPhase {
    companion object {
        operator fun invoke(phases: List<SagaPhase>): Saga<Unit> = Saga(phases) {}
    }
}

interface ReversibleOperation<T> : SagaPhase {
    val operation: Action<T>
    val rollback: Rollback

    companion object {
        operator fun <T> invoke(
            operation: Action<T>,
            revertedBy: Rollback,
        ): ReversibleOperation<T> =
            object : ReversibleOperation<T> {
                override val operation: Action<T> = operation
                override val rollback: Rollback = revertedBy
            }
    }
}

fun interface Rollback {
    /**
     * @throws Rollback.Exception
     */
    operator fun invoke(reason: Any)

    class Exception(cause: Throwable) : RuntimeException(cause)
}

typealias Action<T> = () -> T

inline operator fun <T> Saga<T>.invoke(on: SagaInvoker<T>): T = on(this)

typealias SagaInvoker<T> = (Saga<T>) -> T
