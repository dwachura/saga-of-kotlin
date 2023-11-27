package io.dwsoft.sok

class SagaRunner(private val processor: SagaProcessor) {
    suspend fun <T> execute(saga: Saga<T>): T {
        val (result, rollbacks) = processor.process(saga)
        return if (result.isFailure) {
            val failure = result.exceptionOrNull()!!
            processor.execute(failure, rollbacks)
            throw failure
        } else {
            saga.finalizer()
        }
    }
}

suspend fun <T> Saga<T>.execute(on: SagaRunner): T = on.execute(this)

interface SagaProcessor {
    suspend fun <T> process(saga: Saga<T>): Outcome<T>
    suspend fun execute(reason: Any, rollbacks: List<Rollback>)

    data class Outcome<T>(val result: Result<T>, val rollbacks: List<Rollback>)
}

val SagaProcessor.runner
    get() = SagaRunner(this)
