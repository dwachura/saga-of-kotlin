package io.dwsoft.sok

fun <T> sequentialSagaInvoker(): SagaInvoker<T> =
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

private fun <T> runSaga(saga: Saga<T>): Pair<Result<T>, List<Rollback>> {
    var failure: Throwable? = null
    val rollbacks = saga.phases.asSequence()
        .map {
            runCatching {
                when (it) {
                    is Saga<*> -> runSaga(it)
                    is ReversibleOperation<*> -> it.operation() to listOf(it.rollback)
                }
            }.onFailure { it.throwFatal() }
        }
        .takeWhile {
            failure = it.exceptionOrNull()
            it.isSuccess
        }.fold(emptyList<Rollback>()) { acc, r ->
            acc + r.getOrThrow().second
        }
    val result = failure?.let { Result.failure(it) }
        ?: Result.success(saga.finalizer())
    return result to rollbacks
}
