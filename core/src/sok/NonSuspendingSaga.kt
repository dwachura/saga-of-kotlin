package io.dwsoft.sok

import java.util.concurrent.CompletableFuture

fun <Success, Failure> saga(
    configuration: SagaBuildingScope<Success, Failure>.() -> LateResult<Success>
): Saga.NonSuspending<Success, Failure> =
    SagaBuildingScopeImpl<Success, Failure>().let { buildingScope ->
        val lateResult = buildingScope.configuration()
        val actions = buildingScope.actions
        object : Saga.NonSuspending<Success, Failure> {
            override fun execute(): Saga.Result<Success, Failure> {
                val rollbacks = mutableListOf<() -> Unit>()
                return runCatching {
                    actions.forEach {
                        it()
                        rollbacks += { it.revert() }
                    }
                    with(SagaFinalizerScopeImpl) {
                        Saga.Result.Completed(lateResult.value)
                    }
                }.recoverCatching {
                    when (it) {
                        is SagaActionFailure -> {
                            @Suppress("UNCHECKED_CAST")
                            Saga.Result.Compensated(it.reason as Failure)
                        }
                        else -> throw it
                    }
                }.mapCatching { sagaResult ->
                    if (sagaResult is Saga.Result.Compensated) {
                        rollbacks.forEach { it() }
                    }
                    sagaResult
                }.recoverCatching { throw SagaException(it) }
                    .getOrThrow()
            }

            override fun toReversibleOp(): ReversibleOp.NonSuspending<Saga.Result<Success, Failure>> =
                ReversibleOp.NonSuspending({ execute() }) { TODO("Saga rollback???") }
        }
    }

sealed interface SagaBuildingScope<Success, Failure> {
    // TODO: try with act {} compensatingWith {} style - act should return something different than LateResult then
    fun <T> act(f: SagaActionScope<T, Failure>.() -> T, orCompensate: () -> Unit): LateResult<T>
}

private class SagaBuildingScopeImpl<Success, Failure> : SagaBuildingScope<Success, Failure> {
    val actions = mutableListOf<ReversibleOp.NonSuspending<Saga.Result<*, Failure>>>()

    override fun <T> act(
        f: SagaActionScope<T, Failure>.() -> T,
        orCompensate: () -> Unit
    ): LateResult<T> {
        val lateResult = LateResultImpl<T>()
        actions += ReversibleOp.NonSuspending({
            Saga.Result.Completed(
                SagaActionScopeImpl<T, Failure>().f()
                    .also { lateResult.value.complete(it) }
            )
        }, orCompensate)
        return lateResult
    }
}

sealed interface SagaExecutionScope {
    val <T> LateResult<T>.value: T
        get() = (this as LateResultImpl).value.get()
}

// TODO: think through - looks weird
private data object SagaFinalizerScopeImpl : SagaExecutionScope

sealed interface SagaActionScope<Success, Failure> : SagaExecutionScope {
    fun fail(reason: Failure) {
        throw SagaActionFailure(reason as Any)
    }
}

private class SagaActionScopeImpl<Success, Failure> : SagaActionScope<Success, Failure>

private class SagaActionFailure(val reason: Any) : Throwable()

sealed interface LateResult<T>

private class LateResultImpl<T> : LateResult<T> {
    val value = CompletableFuture<T>()
}
