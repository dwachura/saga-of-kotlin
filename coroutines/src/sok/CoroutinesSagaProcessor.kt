package io.dwsoft.sok

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transformWhile

// TODO: move it to runner logic, abstract traversing, failure check, rollback handling, rollback failure handling
class CoroutinesSagaProcessor : SagaProcessor {
    private val logger = KotlinLogging.logger {}

    override suspend fun <T> process(saga: Saga<T>): SagaProcessor.Outcome<T> {
        val (completed, failed) = saga.phases.asFlow()
            .map {
                when (it) {
                    is Saga<*> -> process(it)
                    is ReversibleOperation<*> -> process(it)
                }
            }.transformWhile {
                emit(it)
                it.result.isSuccess
            }.fold(emptyList<SagaProcessor.Outcome<out Any?>>()) { acc, result ->
                acc + result
            }.partition { it.result.isSuccess }
        if (failed.size > 1) {
            logger.warn {
                val allButFirst = failed.map { it.result.exceptionOrNull() }.drop(1)
                "Although it shouldn't happen, the number of failed phases is ${failed.size}. " +
                        "Only first will be bubbled up. The rest are: $allButFirst"
            }
            // TODO: How to handle multiple failures???
        }
        val failureReason = failed.firstOrNull()?.result?.exceptionOrNull()
        return SagaProcessor.Outcome(
            failureReason?.let { Result.failure(it) } ?: Result.success(saga.finalizer()),
            completed.flatMap { it.rollbacks }
        )
    }

    override suspend fun execute(reason: Any, rollbacks: List<Rollback>) =
        rollbacks.forEach { it.invoke(reason) }

    private suspend fun <T> process(phase: ReversibleOperation<T>): SagaProcessor.Outcome<T> =
        SagaProcessor.Outcome(
            runCatching { phase.operation() }
                .onFailure { it.throwFatal() },
            listOf(phase.rollback)
        )
}
