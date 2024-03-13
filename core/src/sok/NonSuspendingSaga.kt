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
                        is SagaException -> throw it.cause ?: it // fixme: SagaException doesn't need to have a cause
                        else -> throw it
                    }
                }.getOrThrow() to rollbacks.reversed()
            }

            override fun toReversibleOp(): ReversibleOp.NonSuspending<Saga.Result<Success, Failure>> {
                val future = CompletableFuture<List<() -> Unit>>()
                return ReversibleOp.NonSuspending(
                    op = { executeActions().also { future.complete(it.second) }.first },
                    rollback = { future.get().forEach { it.invoke() } },
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

    fun <T> atomic(asynchronous: AsynchronousSagaAction<T, Failure>): LateResult<T>

    fun result(action: SagaActionScope<Success, Failure>.() -> Success): SagaResult<Success>

    fun <T> atomic(saga: Saga<T, Failure>): LateResult<T>

    data class AsynchronousSagaAction<Success, Failure : Any>(
        val action: SagaActionScope<Success, Failure>.() -> AsyncCall<Success>,
        val compensateWith: SagaActionRollbackScope.(actionResult: Success) -> Unit
    )
}

fun <T, Failure : Any> SagaBuildingScope<T, Failure>.asynchronous(
    action: SagaActionScope<T, Failure>.() -> AsyncCall<T>,
    compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit,
): SagaBuildingScope.AsynchronousSagaAction<T, Failure> =
    SagaBuildingScope.AsynchronousSagaAction(action, compensateWith)


private class SagaBuildingScopeImpl<Success, Failure : Any> : SagaBuildingScope<Success, Failure> {
    val actions = mutableListOf<ReversibleOp.NonSuspending<AsyncCall<*>>>()
    lateinit var finalizer: () -> SagaResult<Success>

    private val logger = KotlinLogging.logger {}

    override fun <T> atomic(
        action: SagaActionScope<T, Failure>.() -> T,
        compensateWith: SagaActionRollbackScope.(actionResult: T) -> Unit
    ): LateResult<T> {
        val future = CompletableFuture<T>()
        val lateT = FutureHolder(future)
        val reversibleOp = ReversibleOp.NonSuspending({
            runCatching {
                SagaActionScopeImpl<T, Failure>().action()
            }.fold(
                onSuccess = { future.complete(it) },
                onFailure = {
                    val cause = (it as? ExecutionException)?.cause ?: it
                    future.completeExceptionally(cause)
                }
            )
            return@NonSuspending lateT
        }, { SagaActionRollbackScopeImpl.compensateWith(lateT.getResult()) })
        actions += reversibleOp
        return lateT
    }

    override fun <T> atomic(asynchronous: SagaBuildingScope.AsynchronousSagaAction<T, Failure>): LateResult<T> {
        val (action, compensateWith) = asynchronous
        val lazyFutureHolder: CompletableFuture<FutureHolder<T>> = CompletableFuture()
        val lateT = FutureHolder.cloneOf(lazyFutureHolder)
        val reversibleOp = ReversibleOp.NonSuspending({
            SagaActionScopeImpl<T, Failure>().action().revealed()
                .also { lazyFutureHolder.complete(it) }
            lateT
        }, { SagaActionRollbackScopeImpl.compensateWith(lateT.getResult()) })
        actions += reversibleOp
        // fixme #1: this is not the same as holder returned from action(). Lazy into FutureHolder might be an issue
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
        val lateRollback = FutureHolder<() -> Unit>(future)
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
        get() = revealed().getResult()
}

sealed interface SagaActionScope<Success, Failure : Any> : SagaExecutionScope {
    fun fail(reason: Failure): Nothing

    fun <T> Future<T>.asAsync(): AsyncCall<T>
}

private class SagaActionScopeImpl<Success, Failure : Any> : SagaActionScope<Success, Failure> {
    override fun fail(reason: Failure): Nothing = throw SagaActionFailure(reason)

    override fun <T> Future<T>.asAsync(): AsyncCall<T> = FutureHolder(this)
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
sealed interface LateResult<out T>

private fun <T> LateResult<T>.revealed(): FutureHolder<T> = this as FutureHolder<T>

@SagaDsl
sealed interface AsyncCall<out T> {
    fun cancel(): Boolean
}

private fun <T> AsyncCall<T>.revealed(): FutureHolder<T> = this as FutureHolder<T>

private class FutureHolder<T>(private val lazyFuture: Lazy<Future<T>>) : LateResult<T>, AsyncCall<T> {
    constructor(future: Future<T>) : this(lazyFuture = lazyOf(future))

    private val future: Future<T>
        get() = lazyFuture.takeIf { it.isInitialized() }?.value
            ?: throw UninitializedException

    val isDone: Boolean
        get() = future.isDone

    fun getResult(): T = future.get()

    override fun cancel(): Boolean = future.cancel(true)

    companion object {
        fun <T> cloneOf(lazyOther: CompletableFuture<FutureHolder<T>>): FutureHolder<T> =
            FutureHolder(lazyFuture = lazy {
                // fixme #1: this lazy might be an issue - seems it's not called from finalizer , i.e. returned sub-results of actions are not valid
                lazyOther.takeIf { it.isDone }?.get()?.lazyFuture?.value
                    ?: throw UninitializedException
            })

        val UninitializedException = IllegalStateException(
            """
                |AsyncCall instance has not been fully initialized. This is because it's producer function
                |has not been called or finished yet. This exception mostly indicates an error in internal saga
                |processing - please inform authors providing details for investigation.
            """.trimMargin().lines().joinToString(" ") { it.trimEnd() }
        )
    }
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
    actions: MutableList<ReversibleOp.NonSuspending<AsyncCall<*>>>,
    rollbackStore: MutableList<() -> Unit>
) {
    if (actions.isEmpty()) return

    // invoke each until first fail
    var failedOn: Pair<AsyncCallStage<*>, () -> Unit>? = null
    var active = actions.asSequence()
        .map {
            val (asyncCall, rollback) = it.invoke()
            AsyncCallStage.of(asyncCall) to rollback
        }
        .takeWhile { (stage, rollback) ->
            val shouldStop = stage is AsyncCallStage.Failed || stage is AsyncCallStage.Error
            if (shouldStop) failedOn = stage to rollback
            !shouldStop
        }.toList()
        .let { list -> failedOn?.let { list + it } ?: list }

    while (active.isNotEmpty()) {
        active
            .map { AsyncCallStage.of(it.first.call) as AsyncCallStage<*> to it.second }
            .groupBy { it.first::class }
            .also { results ->
                val processing = results[AsyncCallStage.Processing::class]?.map {
                    it.first as AsyncCallStage.Processing<*> to it.second
                }.orEmpty()
                val success = results[AsyncCallStage.Completed::class]?.map {
                    it.first as AsyncCallStage.Completed<*> to it.second
                }.orEmpty()
                val failed = results[AsyncCallStage.Failed::class]?.map {
                    it.first as AsyncCallStage.Failed<*> to it.second
                }.orEmpty()
                val error = results[AsyncCallStage.Error::class]?.map {
                    it.first as AsyncCallStage.Error<*> to it.second
                }.orEmpty()
                active = processing
                val unsuccessful = listOf(failed, error)
                val nestedRollbacks = failed.mapNotNull { it.first.rollback }
                rollbackStore += success.map { it.second } + nestedRollbacks
                val anyFailed = unsuccessful.any { it.isNotEmpty() }
                if (anyFailed) {
                    active = emptyList()
                    processing.forEach { (stage, rollback) ->
                        if (stage.call.cancel()) rollbackStore += rollback
                    }
                    when {
                        error.isNotEmpty() -> throw error.first().first.cause
                        failed.isNotEmpty() -> throw failed.first().first.cause
                    }
                }
            }
    }
}

private sealed class AsyncCallStage<T>(val call: AsyncCall<T>) {
    class Processing<T>(call: AsyncCall<T>) : AsyncCallStage<T>(call)

    class Failed<T>(
        val cause: InternalSagaEvent,
        val rollback: (() -> Unit)?,
        call: AsyncCall<T>,
    ) : AsyncCallStage<T>(call)

    class Completed<T>(val value: T, call: AsyncCall<T>) : AsyncCallStage<T>(call)

    class Error<T>(val cause: Throwable, call: AsyncCall<T>) : AsyncCallStage<T>(call)

    companion object {
        fun <T> of(call: AsyncCall<T>): AsyncCallStage<T> =
            with(call.revealed()) {
                when {
                    !isDone -> Processing(this)
                    else -> runCatching { getResult() }
                        .fold(
                            onFailure = {
                                when (val cause = (it as? ExecutionException)?.cause ?: it) {
                                    is NestedSagaFailure -> Failed(cause, cause.rollback, this)
                                    is InternalSagaEvent -> Failed(cause, null, this)
                                    else -> Error(cause, this)
                                }
                            },
                            onSuccess = { Completed(it, this) },
                        )
                }
            }
    }
}
