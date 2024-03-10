package io.dwsoft.sok.coroutines

import io.dwsoft.sok.ResultWithSuspendingRollback
import io.dwsoft.sok.ReversibleOp
import kotlinx.coroutines.runBlocking

class SuspendingReversibleOp<T>(
    private val op: suspend () -> T,
    private val rollback: suspend () -> Unit
) : ReversibleOp.Suspending<T> {
    override suspend fun invoke(): ResultWithSuspendingRollback<T> =
        ResultWithSuspendingRollback(op(), rollback)

    override fun toNonSuspending(): ReversibleOp.NonSuspending<T> =
        ReversibleOp.NonSuspending(
            op = { runBlocking { op() } },
            rollback = { runBlocking { rollback() } },
        )
}
