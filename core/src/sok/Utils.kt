package io.dwsoft.sok

fun <T : Throwable> T.throwFatal(): T = takeUnless { it is Error } ?: throw this
