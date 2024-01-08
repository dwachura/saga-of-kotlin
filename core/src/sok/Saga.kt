package io.dwsoft.sok

sealed interface Saga<OpKind : Reversible<*, *>, Result> {
    val ops: Collection<OpKind>
    val finalizer: Finalizer<Result>

    class NonSuspending<T>(
        override val ops: Collection<Reversible.NonSuspendingOp<*>>,
        override val finalizer: Finalizer<T>
    ) : Saga<Reversible.NonSuspendingOp<*>, T>

    class Suspending<T>(
        override val ops: Collection<Reversible.SuspendingOp<*>>,
        override val finalizer: Finalizer<T>
    ) : Saga<Reversible.SuspendingOp<*>, T>
}

typealias Finalizer<T> = () -> T
