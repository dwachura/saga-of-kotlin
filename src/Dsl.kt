package io.dwsoft.sok

fun <T> saga(config: SagaBuilderScope<T>.() -> SagaResult<T>): Saga<T> =
    SagaBuilder<T>().apply { config() }.let {
        Saga(steps = it.steps, finalizer = it.finalizer)
    }

sealed interface SagaBuilderScope<T> {
    fun <R> step(op: () -> R, revertedBy: RevertOperation): StepResult<R>
    fun returning(f: () -> T): SagaResult<T>
}

private class SagaBuilder<T> : SagaBuilderScope<T> {
    val steps = mutableListOf<Revertible<*>>()
    lateinit var finalizer: () -> T

    override fun <R> step(op: () -> R, revertedBy: RevertOperation): StepResult<R> {
        val deferred = DeferredValueImpl<R>()
        steps += Revertible(
            op = { op().also { deferred._value = lazyOf(it) } },
            revertedBy = revertedBy
        )
        return StepResultImpl(deferred)
    }

    override fun returning(f: () -> T): SagaResult<T> {
        val deferred = DeferredValueImpl<T>()
        finalizer = { f().also { deferred._value = lazyOf(it) } }
        return SagaResultImpl(deferred)
    }
}

sealed interface DeferredValue<T> {
    val value: T
}

sealed interface StepResult<T> : DeferredValue<T>

sealed interface SagaResult<T> : DeferredValue<T>

private class DeferredValueImpl<T> : DeferredValue<T> {
    lateinit var _value: Lazy<T>

    override val value: T
        get() = _value.value
}

private class StepResultImpl<T>(value: DeferredValueImpl<T>) : StepResult<T>, DeferredValue<T> by value

private class SagaResultImpl<T>(value: DeferredValueImpl<T>) : SagaResult<T>, DeferredValue<T> by value

fun <T> SagaBuilderScope<*>.step(op: () -> T): StepBuilderScope<T> =
    StepBuilder { step(op = op, revertedBy = it) }

sealed interface StepBuilderScope<T> {
    infix fun revertedBy(f: RevertOperation): StepResult<T>
}

private class StepBuilder<T>(
    private val constructStep: (RevertOperation) -> StepResult<T>
) : StepBuilderScope<T> {
    override fun revertedBy(f: RevertOperation): StepResult<T> = constructStep(f)
}
