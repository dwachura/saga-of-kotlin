package io.dwsoft.sok

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CompletableFuture
import kotlin.math.log

fun <Success, Failure> saga(
    configuration: SagaBuildingScope<Success, Failure>.() -> SagaResult<Success>
): Saga.NonSuspending<Success, Failure> =
    SagaBuildingScopeImpl<Success, Failure>().let { buildingScope ->
        buildingScope.configuration()
        val finalizer = buildingScope.finalizer
        object : Saga.NonSuspending<Success, Failure> {
            private val actions = buildingScope.actions

            override fun execute(): Saga.Result<Success, Failure> {
                val rollbacks = mutableListOf<() -> Unit>()
                return runCatching {
                    actions.forEach {
                        it.invoke()
                        rollbacks += it::revert
                    }
                    val sagaResult = finalizer() as SagaResultImpl<Success>
                    Saga.Result.Completed(sagaResult.value.get())
                }.recoverCatching {
                    when (it) {
                        is SagaActionFailure -> Saga.Result.Compensated(it.reason)
                        is SagaException -> throw it.cause ?: it
                        else -> throw it
                    }
                }.mapCatching { sagaResult ->
                    if (sagaResult is Saga.Result.Compensated) rollbacks.forEach { it.invoke() }
                    sagaResult
                }.recoverCatching {
                    throw SagaException(it)
                }.getOrThrow()
            }

            override fun toReversibleOp(): ReversibleOp.NonSuspending<Saga.Result<Success, Failure>> =
                ReversibleOp.NonSuspending(
                    op = { execute() },
                    rollback = { actions.forEach { it.revert() } }
                )

            private val SagaActionFailure.reason: Failure
                @Suppress("UNCHECKED_CAST")
                get() = rawReason as Failure
        }
    }

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
            Saga.Result.Completed(
                SagaActionScopeImpl<T, Failure>().action()
                    .also { lateResult.value.complete(it) }
            )
        }, { SagaActionRollbackScopeImpl.compensateWith(lateResult.value.get()) })
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
            sagaResult.also {
                it.value.complete(action.execute())
            }
        }
        return sagaResult
    }

    override fun <T> atomic(saga: Saga<T, Failure>): LateResult<T> {
        val op = when (saga) {
            is Saga.NonSuspending -> saga.toReversibleOp()
            is Saga.Suspending -> saga.toReversibleOp().toNonSuspending()
        }
        return atomic(
            action = {
                when (val sagaResult = op()) {
                    is Saga.Result.Completed -> sagaResult.value
                    is Saga.Result.Compensated -> fail(sagaResult.reason)
                }
            },
            compensateWith = { op.revert() }
        )
    }

    private fun <T> (SagaActionScope<T, Failure>.() -> T).execute(): T = invoke(SagaActionScopeImpl())
}

@SagaDsl
sealed interface SagaExecutionScope {
    val <T> LateResult<T>.value: T
        get() = (this as LateResultImpl).value.get()
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
    val value = CompletableFuture<T>()
}

@SagaDsl
sealed interface SagaResult<T>

private class SagaResultImpl<T> : SagaResult<T> {
    val value = CompletableFuture<T>()
}

@DslMarker
annotation class SagaDsl
