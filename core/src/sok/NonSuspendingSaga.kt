package io.dwsoft.sok

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CompletableFuture

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
                // fixme: not list - use stack or linked list (something that can be inverted more efficient)
                val rollbacks = mutableListOf<() -> Unit>()
                return runCatching {
                    val resultsWithRollback = actions.associate { it.second to it.first().rollback }
                    val sagaResult = finalizer().revealed()
                    Saga.Result.Success(sagaResult.value)
                }.recoverCatching {
                    when (it) {
                        is SagaActionFailure -> Saga.Result.Failure(it.reason)
                        is NestedSagaFailure -> {
                            rollbacks += it.rollback
                            Saga.Result.Failure(it.reason)
                        }
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
                SagaActionScopeImpl<T, Failure>().action()
                    .also { lateResult.value = it }
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
        set(value) { _value = CompletableFuture<T>().also { it.complete(value) } }

    private lateinit var _value: CompletableFuture<T>

    val status: Status
        get() = when {
            !_value.isDone -> Status.Processing
            _value.isCompletedExceptionally && !_value.isCancelled -> Status.Failure
            _value.isCancelled -> Status.Cancelled
            else -> Status.Success
        }

    fun isAvailable(): Boolean = status == Status.Processing

    enum class Status {
        Processing, Cancelled, Failure, Success
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

private fun waitCompletion(resultsWithRollbacks: Map<LateResult<*>, () -> Unit>) {
    if (resultsWithRollbacks.isEmpty()) return
    var active = resultsWithRollbacks.mapKeys { it.key.revealed() }.toList()
    val rollbacks = mutableListOf<() -> Unit>()
    while (active.isNotEmpty()) {
        active.groupBy { it.first.status }
            .also {
                active = it[LateResultImpl.Status.Processing]?.takeIf { it.isNotEmpty() }.orEmpty()
                it[LateResultImpl.Status.Success]?.also { rollbacks += it.map { it.second } }
                it[LateResultImpl.Status.Cancelled]?.also { rollbacks += it.map { it.second } }
            }
            .map { (status, results) ->
                when (status) {
                    LateResultImpl.Status.Processing -> active = results.takeIf { it.isNotEmpty() }.orEmpty()
                    LateResultImpl.Status.Cancelled -> TODO()
                    LateResultImpl.Status.Failure -> TODO()
                    LateResultImpl.Status.Success -> TODO()
                }
            }
    }
}
