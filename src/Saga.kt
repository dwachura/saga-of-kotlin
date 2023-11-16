package io.dwsoft.sok

class SagaScope<T>(private val f: suspend SagaScope<T>.() -> T) {
    val compensations = mutableListOf<suspend () -> Unit>()

    suspend fun exec(): T =
        runCatching { f() }
            .onFailure {
//                if (it is Error)
                println("Saga failed. Compensating...")
                compensations.forEach { it() }
            }.getOrThrow()
}

fun <T> saga(f: suspend SagaScope<T>.() -> T): SagaScope<T> = SagaScope(f)

fun <T> SagaScope<*>.phase(f: suspend () -> T): PhaseBuilder<T> = PhaseBuilder(this, f)

class PhaseBuilder<T>(private val sagaScope: SagaScope<*>, private val f: suspend () -> T) {
    suspend infix fun compensate(withAction: suspend (T) -> Unit) =
        f().also { sagaScope.compensations += { withAction(it) } }
}

//inline fun <T, R> T.safelyRun(f: T.() -> R): Result<R> =
//    runCatching(f).onFailure

fun <T : Throwable> T.throwNonCatchable(): T = takeUnless { it is Error } ?: throw this

inline fun <T : Throwable, R> T.recover(f: T.() -> R): R = f()

