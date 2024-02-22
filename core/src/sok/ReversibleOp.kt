package io.dwsoft.sok

sealed interface ReversibleOp<T> {
    interface NonSuspending<T> : ReversibleOp<T>, () -> T {
        fun revert()

        companion object {
            operator fun <T> invoke(op: () -> T, rollback: () -> Unit): NonSuspending<T> =
                object : NonSuspending<T>, () -> T by op {
                    override fun revert() = rollback()
                }
        }
    }

    interface Suspending<T> : ReversibleOp<T>, suspend () -> T {
        suspend fun revert()
        fun toNonSuspending(): NonSuspending<T>
    }
}
