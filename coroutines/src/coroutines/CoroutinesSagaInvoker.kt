package io.dwsoft.sok.coroutines

import io.dwsoft.sok.ReversibleOperation
import io.dwsoft.sok.Rollback
import io.dwsoft.sok.Saga
import io.dwsoft.sok.throwFatal
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transformWhile

private val logger = KotlinLogging.logger("io.dwsoft.sok.coroutines.CoroutinesSagaInvoker")

suspend inline operator fun <T> Saga<T>.invoke(on: SuspendingSagaInvoker<T>): T = on(this)

typealias SuspendingSagaInvoker<T> = suspend (Saga<T>) -> T

fun <T> coroutinesSagaInvoker(scope: CoroutineScope? = null): SuspendingSagaInvoker<T> =
    { saga ->
        val (result, rollbacks) = runSaga(saga)
        if (result.isFailure) {
            val failureReason = requireNotNull(result.exceptionOrNull())
            rollbacks.forEach { it(failureReason) }
            throw failureReason
        } else {
            result.getOrThrow()
        }
    }

private suspend fun <T> runSaga(saga: Saga<T>): Pair<Result<T>, List<Rollback>> {
    val (completed, failed) = saga.phases.asFlow()
        .map {
            when (it) {
                is Saga<*> -> runSaga(it)
                is ReversibleOperation<*> -> {
                    val result = runCatching { it.operation() }
                        .onFailure { e -> e.throwFatal() }
                    result to listOf(it.rollback)
                }
            }
        }.transformWhile {
            emit(it)
            it.first.isSuccess
        }.fold(emptyList<Pair<Result<Any?>, List<Rollback>>>()) { acc, result ->
            acc + result
        }.partition { it.first.isSuccess }
    if (failed.size > 1) {
        logger.warn {
            val allButFirst = failed.map { it.first.exceptionOrNull() }.drop(1)
            "Although it shouldn't happen, the number of failed phases is ${failed.size}. " +
                    "Only first will be bubbled up. The rest are: $allButFirst"
        }
        // TODO: How to handle multiple failures???
    }
    val failureReason = failed.firstOrNull()?.first?.exceptionOrNull()
    val result = failureReason?.let { Result.failure(it) } ?: Result.success(saga.finalizer())
    return result to completed.flatMap { it.second }
}
