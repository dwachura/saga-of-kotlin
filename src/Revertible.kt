package io.dwsoft.sok

class ReversionException(cause: Throwable) : RuntimeException(cause)

/**
 * Represents operation that can be reverted (or compensated in Saga pattern nomenclature).
 */
interface Revertible<T> : Function0<T> {
    val title: String?

    override fun invoke(): T

    /**
     * @throws ReversionException
     */
    fun revert(
        // TODO: what type of reason - should handle exceptions as well as custom error types returned normally
        reason: Any
    )

//    /**
//     * Called when unexpected (non-fatal) exception was thrown during reverting.
//     * Returns false when the error should bubble-up or true otherwise.
//     *
//     * By default, exceptions are rethrown.
//     */
//    fun onReversionException(e: ReversionException): Boolean = false
}

//enum class OnReversionExceptionStrategy {
//    RETHROW,
//}

fun <T> Revertible(
    op: () -> T,
    title: String? = null,
    revertedBy: RevertOperation,
): Revertible<T> =
    object : Revertible<T> {
        override val title: String? = title
        override fun invoke(): T = op()
        override fun revert(reason: Any) = revertedBy(reason)
    }

typealias RevertOperation = (reason: Any) -> Unit
