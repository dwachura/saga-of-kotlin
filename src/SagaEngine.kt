package io.dwsoft.sok

fun <T> SagaElementProcessor.execute(saga: Saga<T>): T = process(saga).result

fun <T> Saga<T>.execute(on: SagaElementProcessor = SimpleSagaElementProcessor()): T = on.execute(this)

interface SagaElementProcessor {
    fun <T> process(saga: Saga<T>): Outcome<T>
    fun <T> process(phase: SagaPhase<T>): Outcome<T>

    data class Outcome<T>(val result: T, val compensatingAction: CompensatingAction)
}

class SimpleSagaElementProcessor : SagaElementProcessor {
    override fun <T> process(saga: Saga<T>): SagaElementProcessor.Outcome<T> {
        var failureReason: Throwable? = null
        val rollbacks = saga.phases.asSequence()
            .map { runCatching {
                when (it) {
                    is Saga<*> -> process(it)
                    is SagaPhase<*> -> process(it)
                }
            }.onFailure { it.throwNonCatchable() } }
            .takeWhile {
                if (it.isFailure) failureReason = it.exceptionOrNull()
                it.isSuccess
            }.toList()
            .map { requireNotNull(it.getOrNull()).compensatingAction }
        val sagaRollback = { reason: Any ->
            rollbacks.forEach { rollback ->
                rollback.invoke(reason) // TODO: throw CompensationException
            }
        }
        return failureReason?.let { // phase failed
            sagaRollback(it)
            throw it
        } ?: SagaElementProcessor.Outcome(saga.finalizer(), sagaRollback)
    }

    override fun <T> process(phase: SagaPhase<T>): SagaElementProcessor.Outcome<T> =
        SagaElementProcessor.Outcome(phase.operation(), phase.rollback)
}
