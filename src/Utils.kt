package io.dwsoft.sok

fun <T : Throwable> T.throwNonCatchable(): T = takeUnless { it is Error } ?: throw this

inline fun <T : Throwable, R> T.recover(f: T.() -> R): R = f()
