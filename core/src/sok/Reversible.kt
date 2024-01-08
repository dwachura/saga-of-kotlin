package io.dwsoft.sok

sealed interface Reversible<Operation, Rollback> {
    class NonSuspendingOp<T>(
        private val op: F<T>,
        private val rollback: Rollback
    ) : F<T> by op, Reversible<F<T>, Rollback>

    class SuspendingOp<T>(
        private val op: SuspendingF<T>,
        private val rollback: SuspendingRollback
    ) : SuspendingF<T> by op, Reversible<SuspendingF<T>, SuspendingRollback>
}

typealias F<T> = () -> T
typealias Rollback = F<Unit>

typealias SuspendingF<T> = suspend () -> T
typealias SuspendingRollback = SuspendingF<Unit>

fun <T> reversible(op: F<T>, rollback: Rollback): Reversible.NonSuspendingOp<T> = Reversible.NonSuspendingOp(op, rollback)

fun <T> suspendingReversible(op: SuspendingF<T>, rollback: SuspendingRollback): Reversible.SuspendingOp<T> =
    Reversible.SuspendingOp(op, rollback)
