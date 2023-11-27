package io.dwsoft.sok

fun <T> saga(config: SagaBuilderScope<T>.() -> SagaResult<T>): Saga<T> =
    SagaBuilder<T>().apply { config() }.let {
        Saga(phases = it.phases, finalizer = it.finalizer)
    }

sealed interface SagaBuilderScope<T> {
    fun <R> phase(operation: Action<R>, revertedBy: Rollback): PhaseResult<R>
    fun returning(finalizer: Action<T>): SagaResult<T>
}

private class SagaBuilder<T> : SagaBuilderScope<T> {
    val phases = mutableListOf<ReversibleOperation<*>>()
    lateinit var finalizer: Action<T>

    override fun <R> phase(operation: Action<R>, revertedBy: Rollback): PhaseResult<R> {
        val deferred = DeferredValueImpl<R>()
        phases += ReversibleOperation(
            operation = { operation().also { deferred._value = lazyOf(it) } },
            revertedBy = revertedBy
        )
        return PhaseResultImpl(deferred)
    }

    override fun returning(finalizer: Action<T>): SagaResult<T> {
        val deferred = DeferredValueImpl<T>()
        this.finalizer = { finalizer().also { deferred._value = lazyOf(it) } }
        return SagaResultImpl(deferred)
    }
}

sealed interface DeferredValue<T> {
    val value: T
}

sealed interface PhaseResult<T> : DeferredValue<T>

sealed interface SagaResult<T> : DeferredValue<T>

private class DeferredValueImpl<T> : DeferredValue<T> {
    lateinit var _value: Lazy<T>

    override val value: T
        get() = _value.value
}

private class PhaseResultImpl<T>(value: DeferredValueImpl<T>) : PhaseResult<T>, DeferredValue<T> by value

private class SagaResultImpl<T>(value: DeferredValueImpl<T>) : SagaResult<T>, DeferredValue<T> by value

fun <T> SagaBuilderScope<*>.phase(operation: Action<T>): PhaseBuilderScope<T> =
    PhaseBuilder { phase(operation = operation, revertedBy = it) }

sealed interface PhaseBuilderScope<T> {
    infix fun revertedBy(rollback: Rollback): PhaseResult<T>
}

private class PhaseBuilder<T>(
    private val buildPhase: (Rollback) -> PhaseResult<T>
) : PhaseBuilderScope<T> {
    override fun revertedBy(rollback: Rollback): PhaseResult<T> =
        buildPhase(rollback)
}
