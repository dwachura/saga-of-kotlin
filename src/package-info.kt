package io.dwsoft.sok

import kotlin.random.Random

/*
 * Notes:
 *  - Phase:
 *      * can be compensated
 *      * all non-fatal errors cause compensation
 *      * how to handle error in compensating action?
 *  - Saga:
 *      * should be composable? (Saga is a Phase?)
 *  - Root Saga cannot be compensated
 *  - Try to separate from third party libs (coroutines as well, add support later)
 *  - onCompensationError() {} is needed
 *      * defined on Saga or/and Phase?
 *      * how to handle errors in simultaneous compensating actions?
 */

suspend fun main() {
    val numbers = mutableListOf<Int>()
    runCatching {
        saga {
            println("Saga started...")
            phase {
                Random.nextInt().also { println("Adding first number $it"); numbers += it }
            } compensate {
                println("Compensating 1"); numbers.remove(it)
            }
            println("numbers: $numbers")
            phase {
                Random.nextInt().also { println("Adding second number $it"); numbers += it }
            } compensate {
                println("Compensating 2"); numbers.remove(it)
            }
            println("numbers: $numbers")
            phase {
                Random.nextInt().also { println("Adding third number $it"); throw RuntimeException("error") }
            } compensate {
                println("Compensating 3"); numbers.remove(it)
            }
            println("numbers: $numbers")
        }.exec()
    }
    println("numbers: $numbers")
}
