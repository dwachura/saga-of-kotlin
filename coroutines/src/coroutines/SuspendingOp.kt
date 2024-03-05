package io.dwsoft.sok.coroutines

import io.dwsoft.sok.ReversibleOp
import kotlinx.coroutines.runBlocking

class SuspendingOp<T>(
    op: suspend () -> T,
    private val rollback: suspend () -> Unit
) : ReversibleOp.SuspendingOld<T>, suspend () -> T by op {
    override suspend fun revert() = rollback()

    override fun toNonSuspending(): ReversibleOp.NonSuspendingOld<T> =
        ReversibleOp.NonSuspendingOld(
            op = { runBlocking { invoke() } },
            rollback = { runBlocking { revert() } },
        )
}
