package io.dwsoft.sok

/**
 * Revertible operation producing result of type T, composed of different revertible steps.
 */
class Saga<T>(
    override val title: String? = null,
    private val steps: List<Revertible<*>>,
    private val finalizer: () -> T,
) : Revertible<T> {
    private val completedSteps = mutableListOf<Revertible<*>>()

    override fun invoke(): T {
        steps.forEach {
            runCatching { it() }
                .fold(
                    onSuccess = { _ -> completedSteps += it },
                    onFailure = { it.throwNonCatchable().recover(::revert) }
                )
        }
        return finalizer()
    }

    override fun revert(reason: Any) {
        completedSteps.forEach {
            runCatching { it.revert(reason) }
                .fold(
                    onSuccess = {},
                    onFailure = { throw it }
                )
        }
        if (reason is Throwable) throw reason
    }
}

fun <T : Throwable> T.throwNonCatchable(): T = takeUnless { it is Error } ?: throw this

inline fun <T : Throwable, R> T.recover(f: T.() -> R): R = f()

