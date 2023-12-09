package io.dwsoft.sok

fun <T> saga(config: SagaBuilderScope<T>.() -> SagaResult<T>): Saga<T> =
    SagaBuilder<T>().apply { config() }.let {
        Saga(phases = it.phases, finalizer = it.finalizer)
    }

sealed interface SagaBuilderScope<T> {
    fun <R> phase(operation: ActionScope<R>, revertedBy: RollbackScope): PhaseResult<R>
    fun returning(finalizer: ActionScope<T>): SagaResult<T>
}

private class SagaBuilder<T> : SagaBuilderScope<T> {
    val phases = mutableListOf<ReversibleOperation<*>>()
    lateinit var finalizer: Action<T>

    override fun <R> phase(operation: ActionScope<R>, revertedBy: RollbackScope): PhaseResult<R> {
        val deferred = DeferredValueImpl<R>()
        phases += ReversibleOperation(
            operation = { DeferredScopeImpl.operation().also { deferred.lazyValue = lazyOf(it) } },
            revertedBy = { DeferredScopeImpl.revertedBy() }
        )
        return PhaseResultImpl(deferred)
    }

    override fun returning(finalizer: ActionScope<T>): SagaResult<T> {
        val deferred = DeferredValueImpl<T>()
        this.finalizer = { DeferredScopeImpl.finalizer().also { deferred.lazyValue = lazyOf(it) } }
        return SagaResultImpl(deferred)
    }
}

sealed interface DeferredValue<T>

sealed interface PhaseResult<T> : DeferredValue<T>

sealed interface SagaResult<T> : DeferredValue<T>

private class DeferredValueImpl<T> : DeferredValue<T> {
    lateinit var lazyValue: Lazy<T>
}

private class PhaseResultImpl<T>(val value: DeferredValueImpl<T>) : PhaseResult<T>, DeferredValue<T>

private class SagaResultImpl<T>(val value: DeferredValueImpl<T>) : SagaResult<T>, DeferredValue<T>

fun <T> SagaBuilderScope<*>.phase(operation: ActionScope<T>): PhaseBuilderScope<T> =
    PhaseBuilder { phase(operation = operation, revertedBy = it) }

sealed interface PhaseBuilderScope<T> {
    infix fun revertedBy(rollback: RollbackScope): PhaseResult<T>
}

private class PhaseBuilder<T>(
    private val buildPhase: (RollbackScope) -> PhaseResult<T>
) : PhaseBuilderScope<T> {
    override fun revertedBy(rollback: RollbackScope): PhaseResult<T> =
        buildPhase(rollback)
}

sealed interface DeferredScope {
    val <T> DeferredValue<T>.value: T
        get() = when (this) {
            is PhaseResultImpl -> value
            is SagaResultImpl -> value
            is DeferredValueImpl -> this
        }.lazyValue.value
}

private data object DeferredScopeImpl : DeferredScope

typealias ActionScope<T> = DeferredScope.() -> T
typealias RollbackScope = DeferredScope.() -> Unit
