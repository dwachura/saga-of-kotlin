package io.dwsoft.sok

sealed interface SagaElement {
    val title: String?
}

/**
 * Revertible operation producing result of type T, composed of different revertible phases.
 */
class Saga<T>(
    val phases: List<SagaElement>,
    override val title: String? = null,
    val finalizer: () -> T,
) : SagaElement {
    companion object {
        operator fun invoke(phases: List<SagaElement>, title: String? = null): Saga<Unit> =
            Saga(phases, title) {}

    }
}

interface SagaPhase<T> : SagaElement {
    override val title: String?
    val operation: () -> T
    val rollback: CompensatingAction

    companion object {
        operator fun <T> invoke(
            operation: () -> T,
            title: String? = null,
            revertedBy: CompensatingAction,
        ): SagaPhase<T> =
            object : SagaPhase<T> {
                override val title: String? = title
                override val operation: () -> T = operation
                override val rollback: CompensatingAction = revertedBy
            }
    }
}

fun interface CompensatingAction : (Any) -> Unit {
    /**
     * @throws CompensatingAction.Exception
     */
    override fun invoke(reason: Any)

    class Exception(cause: Throwable) : RuntimeException(cause)
}
