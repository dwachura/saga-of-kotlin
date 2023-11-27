package io.dwsoft.sok

sealed interface SagaPhase {
    val title: String?
}

/**
 * Revertible operation producing result of type T, composed of different revertible phases.
 */
open class Saga<T>(
    val phases: List<SagaPhase>,
    override val title: String? = null,
    val finalizer: Action<T>,
) : SagaPhase {
    companion object {
        operator fun invoke(phases: List<SagaPhase>, title: String? = null): Saga<Unit> =
            Saga(phases, title) {}

    }
}

interface ReversibleOperation<T> : SagaPhase {
    override val title: String?
    val operation: Action<T>
    val rollback: Rollback

    companion object {
        operator fun <T> invoke(
            operation: suspend () -> T,
            title: String? = null,
            revertedBy: Rollback,
        ): ReversibleOperation<T> =
            object : ReversibleOperation<T> {
                override val title: String? = title
                override val operation: suspend () -> T = operation
                override val rollback: Rollback = revertedBy
            }
    }
}

fun interface Rollback : suspend (Any) -> Unit {
    /**
     * @throws Rollback.Exception
     */
    override suspend fun invoke(reason: Any)

    class Exception(cause: Throwable) : RuntimeException(cause)
}

typealias Action<T> = suspend () -> T
