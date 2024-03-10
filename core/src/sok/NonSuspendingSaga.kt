package io.dwsoft.sok

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

// TODO: debug logs + tracking of saga nesting
// FIXME: order of rollbacks should be from the latest one - change tests and fix implementation
fun <Success, Failure : Any> saga(
    configuration: SagaBuildingScope<Success, Failure>.() -> SagaResult<Success>
): Saga.NonSuspending<Success, Failure> =
    SagaBuildingScopeImpl<Success, Failure>().let { buildingScope ->
        buildingScope.configuration()
        val finalizer = buildingScope.finalizer
        object : Saga.NonSuspending<Success, Failure> {
            private val actions = buildingScope.actions

            override fun execute(): Saga.Result<Success, Failure> =
                runCatching {
                    executeActions()
                }.mapCatching { (result, rollbacks) ->
                    if (result is Saga.Result.Failure) rollbacks.reversed().forEach { it.invoke() }
                    result
                }.recoverCatching {
                    throw SagaException(it)
                }.getOrThrow()

            private fun executeActions(): SagaActionPhaseResult<Success, Failure> {
                val rollbacks = mutableListOf<() -> Unit>()
                return runCatching {
//                    actions.forEach { rollbacks += it.first().rollback }

                    var failedOn: Pair<LateResult<*>, () -> Unit>? = null
                    val resultsWithRollback = actions.map { it.second to it.first() }
                        .takeWhile { (lateResult, resultWithRollback) ->
                            val shouldContinue = lateResult.revealed().status !in
                                    listOf(LateResultImpl.Status.Failure, LateResultImpl.Status.Error)
                            if (!shouldContinue) {
                                failedOn = lateResult to resultWithRollback.rollback
                            }
                            shouldContinue
                        }.associate { it.first to it.second.rollback }
                        .let {
                            if (failedOn != null) it.toMutableMap() + failedOn!! else it
                        }
//
//                    val resultsWithRollback = actions.associate {
//                        // fixme: after recent change exceptions are hold into completable future, so fail-fast is lost
//                        val actionResult = it.first()
//                        val lateResult = it.second.revealed()
//                        if (lateResult.isAvailable() && lateResult.status in
//                            listOf(LateResultImpl.Status.Failure, LateResultImpl.Status.Error)) {
//
//                        }
//                        it.second to actionResult.rollback
//                    }
                    waitCompletion(resultsWithRollback, rollbacks)
                    val sagaResult = finalizer().revealed()
                    Saga.Result.Success(sagaResult.value)
                }.recoverCatching {
                    when (it) {
                        is InternalSagaException -> Saga.Result.Failure(it.reason)
                        is SagaException -> throw it.cause ?: it
                        else -> throw it
                    }
                }.getOrThrow() to rollbacks
            }

            override fun toReversibleOp(): ReversibleOp.NonSuspending<Saga.Result<Success, Failure>> {
                val rollbacks = LateResultImpl<List<() -> Unit>>()
                return ReversibleOp.NonSuspending(
                    op = { executeActions().also { rollbacks.value = it.second }.first },
                    rollback = { rollbacks.value.reversed().forEach { it.invoke() } },
                )
            }

            private val InternalSagaException.reason: Failure
                @Suppress("UNCHECKED_CAST")
                get() = rawReason as Failure
        }
    }

private typealias SagaActionPhaseResult<Success, Failure> = Pair<Saga.Result<Success, Failure>, List<() -> Unit>>

@SagaDsl
sealed interface SagaBuildingScope<Success, Failure : Any> {
    fun <T> atomic(
        action: SagaActionScope<T, Failure>.() -> T,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit
    ): LateResult<T>

    fun result(action: SagaActionScope<Success, Failure>.() -> Success): SagaResult<Success>

    fun <T> atomic(saga: Saga<T, Failure>): LateResult<T>
}

private class SagaBuildingScopeImpl<Success, Failure : Any> : SagaBuildingScope<Success, Failure> {
//    val actions = mutableListOf<ReversibleOp.NonSuspending<Saga.Result<*, Failure>>>()
    val actions = mutableListOf<SagaActionWithLateResult<*, Failure>>()
    lateinit var finalizer: () -> SagaResult<Success>

    private val logger = KotlinLogging.logger {}

    override fun <T> atomic(
        action: SagaActionScope<T, Failure>.() -> T,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit
    ): LateResult<T> {
        val lateResult = LateResultImpl<T>()
        val reversibleOp = ReversibleOp.NonSuspending<Saga.Result<*, Failure>>({
            Saga.Result.Success(
                runCatching {
                    SagaActionScopeImpl<T, Failure>().action()
                }.fold(
                    onSuccess = { lateResult.value = it },
                    onFailure = { lateResult.completeExceptionally(it) }
                )
            )
        }, { SagaActionRollbackScopeImpl.compensateWith(lateResult.value) })
//        actions += reversibleOp
        actions += (reversibleOp to lateResult)
        return lateResult
    }

    override fun result(action: SagaActionScope<Success, Failure>.() -> Success): SagaResult<Success> {
        if (::finalizer.isInitialized) {
            logger.warn {
                """
                    |Result function already set and will be overwritten! Almost certainly this is not desirable
                    |and caused by duplicated invocation of result function into single building scope. It should
                    |be fixed, as in future releases this behavior may be reported as an exception.
                """.trimMargin().lines().joinToString(" ") { it.trimEnd() }
            }
        }
        val sagaResult = SagaResultImpl<Success>()
        finalizer = {
            sagaResult.also { it.value = action.execute() }
        }
        return sagaResult
    }

    override fun <T> atomic(saga: Saga<T, Failure>): LateResult<T> {
        val op = when (saga) {
            is Saga.NonSuspending -> saga.toReversibleOp()
            is Saga.Suspending -> saga.toReversibleOp().toNonSuspending()
        }
        val lateRollback = LateResultImpl<() -> Unit>()
        return atomic(
            action = {
                val (result, rollback) = op()
                lateRollback.value = rollback
                when (result) {
                    is Saga.Result.Success -> result.value
                    is Saga.Result.Failure -> throw NestedSagaFailure(result.reason, rollback)
                }
            },
            compensateWith = { lateRollback.value.invoke() }
        )
    }

    private fun <T> (SagaActionScope<T, Failure>.() -> T).execute(): T = invoke(SagaActionScopeImpl())
}

@SagaDsl
sealed interface SagaExecutionScope {
    val <T> LateResult<T>.value: T
        get() = revealed().value
}

sealed interface SagaActionScope<Success, Failure : Any> : SagaExecutionScope {
    fun fail(reason: Failure): Nothing
}

private class SagaActionScopeImpl<Success, Failure : Any> : SagaActionScope<Success, Failure> {
    override fun fail(reason: Failure): Nothing {
        throw SagaActionFailure(reason)
    }
}

private sealed class InternalSagaException(val rawReason: Any) : Error(
    """
        |Saga's action failed and compensation process will trigger. Usage of instances of this class is strictly
        |internal - it's used to differentiate between action's expected failures and unexpected errors. That's why
        |even though it's a subtype of Error class it doesn't indicate serious problem. It's to make it harder for
        |a developer to accidentally catch it.
    """.trimMargin().lines().joinToString(" ") { it.trimEnd() }
)

private class SagaActionFailure(rawReason: Any) : InternalSagaException(rawReason)

private class NestedSagaFailure(rawReason: Any, val rollback: () -> Unit) : InternalSagaException(rawReason)

sealed interface SagaActionRollbackScope : SagaExecutionScope

private data object SagaActionRollbackScopeImpl : SagaActionRollbackScope

@SagaDsl
sealed interface LateResult<T>

private fun <T> LateResult<T>.revealed(): LateResultImpl<T> = this as LateResultImpl<T>

private class LateResultImpl<T> : LateResult<T> {
    var value: T
        get() = _value.get()
        set(value) { _value.complete(value) }

    private val _value: CompletableFuture<T> = CompletableFuture<T>()

    val status: Status
        get() = when {
            !_value.isDone -> Status.Processing
            _value.isCancelled -> Status.Cancelled
            _value.isCompletedExceptionally -> {
                val exception = exception!!
                when {
                    exception is InternalSagaException -> Status.Failure
                    else -> Status.Error
                }
            }
            else -> Status.Success
        }

    fun isAvailable(): Boolean = status == Status.Processing

    val exception: Throwable?
        get() = runCatching { _value.get() }.exceptionOrNull()
            ?.let { if (it is ExecutionException) it.cause else it }

    fun cancel(): Boolean = _value.cancel(true)

    fun completeExceptionally(throwable: Throwable?): Boolean =
        _value.completeExceptionally(throwable)

    enum class Status {
        Processing, Cancelled, Failure, Success, Error
    }
}

@SagaDsl
sealed interface SagaResult<T>

private fun <T> SagaResult<T>.revealed(): SagaResultImpl<T> = this as SagaResultImpl<T>

private class SagaResultImpl<T> : SagaResult<T> {
    var value: T
        get() = _value.get()
        set(value) { _value = CompletableFuture<T>().also { it.complete(value) } }

    private lateinit var _value: CompletableFuture<T>
}

@DslMarker
annotation class SagaDsl

private typealias SagaActionWithLateResult<Success, Failure> =
        Pair<ReversibleOp.NonSuspending<Saga.Result<Success, Failure>>, LateResult<Success>>

private fun waitCompletion(
    resultsWithRollbacks: Map<LateResult<*>, () -> Unit>,
    rollbackStore: MutableList<() -> Unit>,
) {
    if (resultsWithRollbacks.isEmpty()) return
    var active = resultsWithRollbacks.mapKeys { it.key.revealed() }.toList()
    while (active.isNotEmpty()) {
        active.groupBy { it.first.status }
            .also { results ->
                active = results[LateResultImpl.Status.Processing]?.takeIf { it.isNotEmpty() }.orEmpty()
                val (success, cancelled, failed, error) = listOf(
                    results[LateResultImpl.Status.Success],
                    results[LateResultImpl.Status.Cancelled],
                    results[LateResultImpl.Status.Failure],
                    results[LateResultImpl.Status.Error],
                ).map { it.orEmpty() }
                val unsuccessful = listOf(cancelled, failed, error)
                val nestedRollbacks = failed.mapNotNull { (it.first.exception as? NestedSagaFailure)?.rollback }
                rollbackStore += (success.map { it.second } + nestedRollbacks)
                val shouldCancelActive = unsuccessful.any { it.isNotEmpty() }
                if (shouldCancelActive) {
                    active.forEach {
                        if (!it.first.cancel()) rollbackStore += it.second
                    }
                    active = emptyList()
                }
                when {
                    error.isNotEmpty() -> throw error.first().first.exception!!
                    cancelled.isNotEmpty() -> throw cancelled.first().first.exception!!
                    failed.isNotEmpty() -> throw failed.first().first.exception!!
                }
            }
    }
}
