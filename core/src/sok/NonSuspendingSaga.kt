package io.dwsoft.sok

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

// TODO: debug logs + tracking of saga nesting
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
                    if (result is Saga.Result.Failure) rollbacks.forEach { it.invoke() }
                    result
                }.recoverCatching {
                    throw SagaException(it)
                }.getOrThrow()

            private fun executeActions(): SagaActionPhaseResult<Success, Failure> {
                val rollbacks = mutableListOf<() -> Unit>()
                return runCatching {
                    waitCompletion(actions, rollbacks)
                    val sagaResult = finalizer().revealed()
                    Saga.Result.Success(sagaResult.value)
                }.recoverCatching {
                    when (it) {
                        is InternalSagaEvent -> Saga.Result.Failure(it.reason)
                        is SagaException -> throw it.cause ?: it
                        else -> throw it
                    }
                }.getOrThrow() to rollbacks.reversed()
            }

            override fun toReversibleOp(): ReversibleOp.NonSuspending<Saga.Result<Success, Failure>> {
                val future = CompletableFuture<List<() -> Unit>>()
                val rollbacks = FutureHolder<List<() -> Unit>>()
                rollbacks.future = future
                return ReversibleOp.NonSuspending(
                    op = { executeActions().also { future.complete(it.second) }.first },
                    rollback = { rollbacks.future.get().forEach { it.invoke() } },
                )
            }

            private val InternalSagaEvent.reason: Failure
                @Suppress("UNCHECKED_CAST")
                get() = context as Failure
        }
    }

private typealias SagaActionPhaseResult<Success, Failure> = Pair<Saga.Result<Success, Failure>, List<() -> Unit>>

@SagaDsl
sealed interface SagaBuildingScope<Success, Failure : Any> {
    fun <T> atomic(
        action: SagaActionScope<T, Failure>.() -> T,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit,
    ): LateResult<T>

    fun <T> asyncAtomic(
        action: SagaActionScope<T, Failure>.() -> Future<T>,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit,
    ): LateResult<T>

    fun result(action: SagaActionScope<Success, Failure>.() -> Success): SagaResult<Success>

    fun <T> atomic(saga: Saga<T, Failure>): LateResult<T>
}

private class SagaBuildingScopeImpl<Success, Failure : Any> : SagaBuildingScope<Success, Failure> {
    val actions = mutableListOf<ReversibleOp.NonSuspending<Future<*>>>()
    lateinit var finalizer: () -> SagaResult<Success>

    private val logger = KotlinLogging.logger {}

    override fun <T> atomic(
        action: SagaActionScope<T, Failure>.() -> T,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit
    ): LateResult<T> {
        val future = CompletableFuture<T>()
        val lateT = FutureHolder<T>()
        lateT.future = future
        val reversibleOp = ReversibleOp.NonSuspending({
            runCatching {
                SagaActionScopeImpl<T, Failure>().action()
            }.fold(
                onSuccess = { future.complete(it) },
                onFailure = {
                    future.completeExceptionally(if (it is ExecutionException) it.cause else it)
                }
            )
            future
        }, { SagaActionRollbackScopeImpl.compensateWith(lateT.future.get()) })
        actions += reversibleOp
        return lateT
    }

    override fun <T> asyncAtomic(
        action: SagaActionScope<T, Failure>.() -> Future<T>,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit
    ): LateResult<T> {
        val lateT = FutureHolder<T>()
        val reversibleOp = ReversibleOp.NonSuspending({
            SagaActionScopeImpl<T, Failure>().action()
                .also { lateT.future = it }
        }, { SagaActionRollbackScopeImpl.compensateWith(lateT.future.get()) })
        actions += reversibleOp
        return lateT
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
        val future = CompletableFuture<() -> Unit>()
        val lateRollback = FutureHolder<() -> Unit>()
        lateRollback.future = future
        return atomic(
            action = {
                val (result, rollback) = op()
                future.complete(rollback)
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
        get() = revealed().future.get()
}

sealed interface SagaActionScope<Success, Failure : Any> : SagaExecutionScope {
    fun fail(reason: Failure): Nothing
}

private class SagaActionScopeImpl<Success, Failure : Any> : SagaActionScope<Success, Failure> {
    override fun fail(reason: Failure): Nothing {
        throw SagaActionFailure(reason)
    }
}

private sealed class InternalSagaEvent(val context: Any) : Error(
    """
        |Saga internal event occurred. Usage of instances of this class is strictly internal - it's used to
        |differentiate between action's expected status changes and unexpected errors. That's why even though it's
        |a subtype of Error class it doesn't indicate serious problem. It's to make it harder for a developer to
        |accidentally catch it.
    """.trimMargin().lines().joinToString(" ") { it.trimEnd() }
)

private class SagaActionFailure(context: Any) : InternalSagaEvent(context)

private class NestedSagaFailure(context: Any, val rollback: () -> Unit) : InternalSagaEvent(context)

sealed interface SagaActionRollbackScope : SagaExecutionScope

private data object SagaActionRollbackScopeImpl : SagaActionRollbackScope

@SagaDsl
sealed interface LateResult<T>

private fun <T> LateResult<T>.revealed(): FutureHolder<T> = this as FutureHolder<T>

private class FutureHolder<T> : LateResult<T> {
    lateinit var future: Future<T>
}

@SagaDsl
sealed interface SagaResult<T>

private fun <T> SagaResult<T>.revealed(): SagaResultImpl<T> = this as SagaResultImpl<T>

private class SagaResultImpl<T> : SagaResult<T> {
    var value: T
        get() = _value.get()
        set(value) {
            _value = CompletableFuture<T>().also { it.complete(value) }
        }

    private lateinit var _value: CompletableFuture<T>
}

@Target(AnnotationTarget.CLASS, AnnotationTarget.TYPE)
@DslMarker
annotation class SagaDsl

private fun waitCompletion(
    actions: MutableList<ReversibleOp.NonSuspending<Future<*>>>,
    rollbackStore: MutableList<() -> Unit>
) {
    if (actions.isEmpty()) return

    // invoke each until first fail
    var failedOn: Pair<ActionStage<*>, () -> Unit>? = null
    val stagesWithRollbacks = actions.asSequence()
        .map {
            val (future, rollback) = it.invoke()
            ActionStage.of(future) to rollback
        }
        .takeWhile { (result, rollback) ->
            val shouldStop = result is ActionStage.Failed || result is ActionStage.Error
            if (shouldStop) {
                failedOn = result to rollback
            }
            !shouldStop
        }.toList()
        .let { if (failedOn != null) it + failedOn!! else it }

    var active = stagesWithRollbacks
    while (active.isNotEmpty()) {
        active.map { ActionStage.of(it.first.future) to it.second }
            .groupBy { it.first::class }
            .also { results ->
                val processing = results[ActionStage.Processing::class]?.map {
                    it.first as ActionStage.Processing<*> to it.second
                }.orEmpty()
                val success = results[ActionStage.Completed::class]?.map {
                    it.first as ActionStage.Completed<*> to it.second
                }.orEmpty()
                val failed = results[ActionStage.Failed::class]?.map {
                    it.first as ActionStage.Failed<*> to it.second
                }.orEmpty()
                val error = results[ActionStage.Error::class]?.map {
                    it.first as ActionStage.Error<*> to it.second
                }.orEmpty()
                active = processing
                val unsuccessful = listOf(failed, error)
                val nestedRollbacks = failed.mapNotNull { it.first.rollback }
                rollbackStore += success.map { it.second } + nestedRollbacks
                val shouldCancelActive = unsuccessful.any { it.isNotEmpty() }
                if (shouldCancelActive) {
                    processing.filterNot { it.first.cancel() }
                        .forEach { rollbackStore += it.second }
                    active = emptyList()
                }
                when {
                    error.isNotEmpty() -> throw error.first().first.cause
                    failed.isNotEmpty() -> throw failed.first().first.cause
                }
            }
    }
}

private sealed class ActionStage<T>(val future: Future<T>) {
    class Processing<T>(future: Future<T>) : ActionStage<T>(future) {
        fun cancel(): Boolean = future.cancel(true)
    }

    class Failed<T>(val cause: InternalSagaEvent, val rollback: (() -> Unit)?, future: Future<T>) :
        ActionStage<T>(future)

    class Completed<T>(val value: T, future: Future<T>) : ActionStage<T>(future)

    class Error<T>(val cause: Throwable, future: Future<T>) : ActionStage<T>(future)

    companion object {
        fun <T> of(future: Future<T>): ActionStage<T> =
            when {
                !future.isDone -> Processing(future)
                else -> runCatching { future.get() }
                    .fold(
                        onFailure = {
                            when (val cause = (it as? ExecutionException)?.cause ?: it) {
                                is InternalSagaEvent ->
                                    Failed(cause, (cause as? NestedSagaFailure)?.rollback, future)
                                else -> Error(cause, future)
                            }
                        },
                        onSuccess = { Completed(it, future) },
                    )
            }
    }
}
