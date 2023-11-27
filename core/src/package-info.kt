package io.dwsoft

/*
 *  TODO:
 *    Notes:
 *    - Phase:
 *        * can be compensated
 *        * all non-fatal errors cause compensation
 *        * how to handle error in compensating action?
 *    - Saga:
 *        * should be composable? (Saga is a Phase?)
 *    - Root Saga cannot be compensated
 *    - Try to separate from third party libs (coroutines as well, add support later)
 *    - onCompensationError() {} is needed
 *        * defined on Saga or/and Phase?
 *        * how to handle errors in simultaneous compensating actions?
 *    - Saga builder must not be suspended (to be confirmed)

 *    - Reversible.title: String?
 *    - Steps indexed in saga
 *    - RevertingException handling mode for Saga - interface with TreatAsFatal, Ignore, Journaling (for future)
 *    implementations (still log internally every time)

 *    - DSL:
 *        - lazy step result usable in other steps (val t: Lazy<T> = step { T } revertedBy { ... }
 *        - step(Saga<T>): Lazy<T> (require saga composition)
 *        - separate business from reverting definitions style
 *         - suspensions, coroutines and how to define and run parallel steps

 *        nice Dsl:
 *            saga {
 *                val x: Lazy<Int> = step { 1 } revertedBy { ... }
 *                val y: Lazy<Int> = step { x.value + 2 } revertedBy { ... }
 *                result(x + y)
 *            }

 *        perfect Dsl:
 *            saga {
 *                val x: Int = step { 1 } revertedBy { ... }
 *                val y: Int = step { x + 2 } revertedBy { ... }
 *                x + y
 *            }
 */
