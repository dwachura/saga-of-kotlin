package io.dwsoft.sok

fun <T> saga(config: SagaBuilderScope<T>.() -> SagaResult<T>): Saga<T> =
    SagaBuilder<T>().apply { config() }.let {
        Saga(phases = it.phases, finalizer = it.finalizer)
    }

sealed interface SagaBuilderScope<T> {
    fun <R> phase(operation: () -> R, revertedBy: CompensatingAction): PhaseResult<R>
    fun returning(finalizer: () -> T): SagaResult<T>
}

private class SagaBuilder<T> : SagaBuilderScope<T> {
    val phases = mutableListOf<SagaPhase<*>>()
    lateinit var finalizer: () -> T

    override fun <R> phase(operation: () -> R, revertedBy: CompensatingAction): PhaseResult<R> {
        val deferred = DeferredValueImpl<R>()
        phases += SagaPhase(
            operation = { operation().also { deferred._value = lazyOf(it) } },
            revertedBy = revertedBy
        )
        return PhaseResultImpl(deferred)
    }

    override fun returning(finalizer: () -> T): SagaResult<T> {
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

fun <T> SagaBuilderScope<*>.phase(operation: () -> T): PhaseBuilderScope<T> =
    PhaseBuilder { phase(operation = operation, revertedBy = it) }

sealed interface PhaseBuilderScope<T> {
    infix fun compensatedBy(rollback: CompensatingAction): PhaseResult<T>
}

private class PhaseBuilder<T>(
    private val constructStep: (CompensatingAction) -> PhaseResult<T>
) : PhaseBuilderScope<T> {
    override fun compensatedBy(rollback: CompensatingAction): PhaseResult<T> =
        constructStep(rollback)
}
