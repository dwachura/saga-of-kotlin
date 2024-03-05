package io.dwsoft.sok

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CompletableFuture

// FIXME: order of rollbacks should be from the latest one - change tests and fix implementation
fun <Success, Failure> saga(
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
                    if (result is Saga.Result.Failure) rollbacks.forEach { it.invoke() }
                    result
                }.recoverCatching {
                    throw SagaException(it)
                }.getOrThrow()

            private fun executeActions(): SagaActionPhaseResult<Success, Failure> {
                val rollbacks = mutableListOf<() -> Unit>()
                return runCatching {
                    actions.forEach { rollbacks += it.invoke().second }
                    val sagaResult = finalizer() as SagaResultImpl<Success>
                    Saga.Result.Success(sagaResult.value)
                }.recoverCatching {
                    when (it) {
                        is SagaActionFailure -> Saga.Result.Failure(it.reason)
                        is SagaException -> throw it.cause ?: it
                        else -> throw it
                    }
                }.getOrThrow() to rollbacks
            }

            override fun toReversibleOp(): ReversibleOp.NonSuspending<Saga.Result<Success, Failure>> {
                val rollbacks = LateResultImpl<List<() -> Unit>>()
                return ReversibleOp.NonSuspending(
                    op = { executeActions().also { rollbacks.value = it.second }.first },
                    rollback = { rollbacks.value.forEach { it.invoke() } },
                )
            }

            private val SagaActionFailure.reason: Failure
                @Suppress("UNCHECKED_CAST")
                get() = rawReason as Failure
        }
    }

private typealias SagaActionPhaseResult<Success, Failure> = Pair<Saga.Result<Success, Failure>, List<() -> Unit>>

@SagaDsl
sealed interface SagaBuildingScope<Success, Failure> {
    fun <T> atomic(
        action: SagaActionScope<T, Failure>.() -> T,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit
    ): LateResult<T>

    fun result(action: SagaActionScope<Success, Failure>.() -> Success): SagaResult<Success>

    fun <T> atomic(saga: Saga<T, Failure>): LateResult<T>
}

private class SagaBuildingScopeImpl<Success, Failure> : SagaBuildingScope<Success, Failure> {
    val actions = mutableListOf<ReversibleOp.NonSuspending<Saga.Result<*, Failure>>>()
    lateinit var finalizer: () -> SagaResult<Success>

    private val logger = KotlinLogging.logger {}

    override fun <T> atomic(
        action: SagaActionScope<T, Failure>.() -> T,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit
    ): LateResult<T> {
        val lateResult = LateResultImpl<T>()
        actions += ReversibleOp.NonSuspending({
            Saga.Result.Success(
                SagaActionScopeImpl<T, Failure>().action()
                    .also { lateResult.value = it }
            )
        }, { SagaActionRollbackScopeImpl.compensateWith(lateResult.value) })
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
        val rollback = LateResultImpl<() -> Unit>()
        return atomic(
            action = {
                val operationResult = try {
                    op()
                } catch (e: Exception) {
                    throw e
                }
//                val operationResult = op()
                rollback.value = operationResult.second
                when (val sagaResult = operationResult.first) {
                    is Saga.Result.Success -> sagaResult.value
                    is Saga.Result.Failure -> fail(sagaResult.reason) // FIXME: we lose nested rollback here
                }
            },
            compensateWith = { rollback.value.invoke() }
        )
    }

    private fun <T> (SagaActionScope<T, Failure>.() -> T).execute(): T = invoke(SagaActionScopeImpl())
}

@SagaDsl
sealed interface SagaExecutionScope {
    val <T> LateResult<T>.value: T
        get() = (this as LateResultImpl).value
}

sealed interface SagaActionScope<Success, Failure> : SagaExecutionScope {
    fun fail(reason: Failure): Nothing
}

private class SagaActionScopeImpl<Success, Failure> : SagaActionScope<Success, Failure> {
    override fun fail(reason: Failure): Nothing {
        throw SagaActionFailure(reason as Any)
    }
}

private class SagaActionFailure(val rawReason: Any) : Error(
    """
        |Saga's action failed and compensation process will trigger. Usage of instances of this class is strictly
        |internal - it's used to differentiate between action's expected failures and unexpected errors. That's why
        |even though it's a subtype of Error class it doesn't indicate serious problem. It's to make it harder for
        |a developer to accidentally catch it.
    """.trimMargin().lines().joinToString(" ") { it.trimEnd() }
)

sealed interface SagaActionRollbackScope : SagaExecutionScope

private data object SagaActionRollbackScopeImpl : SagaActionRollbackScope

@SagaDsl
sealed interface LateResult<T>

private class LateResultImpl<T> : LateResult<T> {
    var value: T
        get() = _value.get()
        set(value) { _value = CompletableFuture<T>().also { it.complete(value) } }

    private lateinit var _value: CompletableFuture<T>
}

@SagaDsl
sealed interface SagaResult<T>

private class SagaResultImpl<T> : SagaResult<T> {
    var value: T
        get() = _value.get()
        set(value) { _value = CompletableFuture<T>().also { it.complete(value) } }

    private lateinit var _value: CompletableFuture<T>
}

@DslMarker
annotation class SagaDsl
