package io.dwsoft.sok

import io.kotest.assertions.asClue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.comparables.shouldNotBeGreaterThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifySequence
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTimedValue

// TODO: maybe group test cases by execution and rollback phases
class NonSuspendingSagaTest : FreeSpec({
    "Action results are usable in..." - {
        "...other actions" {
            val recorder: (Int) -> Unit = spyk()
            saga<Unit, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                atomic({ recorder(v.value) }, compensateWith = {})
                result {}
            }.execute()

            verify(exactly = 1) { recorder(1) }
        }

        "...rollbacks" {
            val recorder: (Int) -> Unit = spyk()
            saga {
                val v = atomic({ 1 }, compensateWith = {})
                atomic({ 2 }, compensateWith = {
                    recorder(v.value)
                    recorder(it)
                })
                result { fail(Unit) }
            }.execute()

            verify(exactly = 1) { recorder(1) }
            verify(exactly = 1) { recorder(2) }
        }

        "...completions" {
            val recorder: (Int) -> Unit = spyk()
            saga<Unit, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                result { recorder(v.value) }
            }.execute()

            verify(exactly = 1) { recorder(1) }
        }
    }

    "Saga compensates completed actions when..." - {
        "...action fails" {
            val expectedReason = Any()
            val expectedRollback = RollbackRecorder<Int>()
            val unexpectedRollback = RollbackRecorder<Int>()
            val saga = saga {
                atomic({ 1 }, compensateWith = expectedRollback)
                atomic({ 2 }, compensateWith = expectedRollback)
                atomic({ fail(expectedReason) }, compensateWith = unexpectedRollback)
                result {}
            }

            val result = saga.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<*>>()
                .reason shouldBe expectedReason
            expectedRollback.shouldBeCalledWith(2, 1)
            unexpectedRollback.shouldNotBeCalled()
        }

        "...completion fails" {
            val expectedReason = Any()
            val rollback = RollbackRecorder<Int>()
            val saga = saga {
                atomic({ 1 }, compensateWith = rollback)
                atomic({ 2 }, compensateWith = rollback)
                result { fail(expectedReason) }
            }

            val result = saga.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<*>>()
                .reason shouldBe expectedReason
            rollback.shouldBeCalledWith(2, 1)
        }

        "Rollbacks are called when action throws non-fatal exception" {
            shouldThrowAny { TODO("Is this behavior correct/needed???") }
        }

        "Rollbacks are called when completion throws non-fatal exception" {
            shouldThrowAny { TODO("Is this behavior correct/needed???") }
        }
    }

    "Saga with successful actions finishes successfully" {
        val result = saga<Int, Nothing> {
            val v1 = atomic({ 1 }, compensateWith = {})
            val v2 = atomic({ 2 }, compensateWith = {})
            result { v1.value + v2.value }
        }.execute()

        result.shouldBeInstanceOf<Saga.Result.Success<Int>>()
            .value shouldBe 3
    }

    "Saga fails fast when exception is thrown from..." - {
        "...action" {
            val expectedException = RuntimeException()
            val rollback = RollbackRecorder<Any>()
            val saga = saga<Unit, Unit> {
                atomic({}, compensateWith = rollback)
                atomic({ throw expectedException }, compensateWith = {})
                result {}
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedException
            rollback.shouldNotBeCalled()
        }

        "...rollback" {
            val expectedException = RuntimeException()
            val rollback = RollbackRecorder<Int>()
            val saga = saga<Unit, Unit> {
                atomic({ 1 }, compensateWith = rollback)
                atomic({}, compensateWith = { throw expectedException })
                atomic({ 2 }, compensateWith = rollback)
                atomic({ fail(Unit) }, compensateWith = {})
                result {}
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedException
            rollback.shouldBeCalledWith(2)
        }

        "... completion" {
            val expectedException = RuntimeException()
            val rollback = RollbackRecorder<Int>()
            val saga = saga<Unit, Unit> {
                atomic({ 1 }, compensateWith = rollback)
                atomic({ 2 }, compensateWith = rollback)
                atomic({ 3 }, compensateWith = rollback)
                result { throw expectedException }
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedException
            rollback.shouldNotBeCalled()
        }
    }

    "Saga nesting" - {
        "Successful nested saga returns value" {
            val saga = saga<Int, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                result { v.value }
            }

            val result = saga<Int, Nothing> {
                val v = atomic(saga)
                result { v.value }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Success<Int>>()
                .value shouldBe 1
        }

        "Nested saga is compensated" {
            val rollback = RollbackRecorder<Int>()
            val saga = saga<Int, Nothing> {
                val v1 = atomic({ 1 }, compensateWith = rollback)
                val v2 = atomic({ 2 }, compensateWith = rollback)
                result { v1.value + v2.value }
            }

            val result = saga<Int, Unit> {
                atomic(saga)
                result { fail(Unit) }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<Unit>>()
            rollback.shouldBeCalledWith(2, 1)
        }

        "Failed nested saga triggers compensation" {
            val rollback = RollbackRecorder<Int>()
            val saga = saga<Int, String> {
                val v1 = atomic({ 2 }, compensateWith = rollback)
                val v2 = atomic({ fail("Test") }, compensateWith = rollback)
                result { v1.value + v2.value }
            }

            val result = saga<Int, String> {
                atomic({ 1 }, compensateWith = rollback)
                val v = atomic(saga)
                atomic({ 3 }, compensateWith = rollback)
                result { v.value }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                .reason shouldBe "Test"
            rollback.shouldBeCalledWith(2, 1)
        }

        "Saga fails fast when errors are thrown from nested saga's..." - {
            "...action" {
                val expectedException = RuntimeException()
                val rollback = RollbackRecorder<Int>()
                val nestedSaga = saga<Unit, Nothing> {
                    atomic({ 2 }, compensateWith = rollback)
                    atomic({ throw expectedException }, compensateWith = rollback)
                    atomic({ 3 }, compensateWith = rollback)
                    result {}
                }
                val saga = saga<Unit, String> {
                    atomic({ 1 }, compensateWith = rollback)
                    atomic(nestedSaga)
                    result {}
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }.cause shouldBe expectedException
                rollback.shouldNotBeCalled()
            }

            "...completion" {
                val expectedException = RuntimeException()
                val rollback = RollbackRecorder<Int>()
                val nestedSaga = saga<Unit, Nothing> {
                    atomic({ 2 }, compensateWith = rollback)
                    result { throw expectedException }
                }
                val saga = saga<Unit, String> {
                    atomic({ 1 }, compensateWith = rollback)
                    atomic(nestedSaga)
                    result {}
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }.cause shouldBe expectedException
                rollback.shouldNotBeCalled()
            }

            "...rollback" {
                val expectedException = RuntimeException()
                val rollback = RollbackRecorder<Int>()
                val nestedSaga = saga<Unit, Nothing> {
                    atomic({ 2 }, compensateWith = rollback)
                    atomic({ 3 }, compensateWith = { throw expectedException })
                    atomic({ 4 }, compensateWith = rollback)
                    result {}
                }
                val saga = saga<Unit, Unit> {
                    atomic({ 1 }, compensateWith = rollback)
                    atomic(nestedSaga)
                    atomic({ 5 }, compensateWith = rollback)
                    result { fail(Unit) }
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }.cause shouldBe expectedException
                rollback.shouldBeCalledWith(5, 4)
            }
        }
    }

    // TODO:
    //  * check behavior of compensation of parallel actions of
    //  nested saga when the completes only partially
    //  * what happens when failed action cancels another and the later fails during this -
    //  does cancel() returns false? Is rollback of such action skipped (as it should)?
    "Parallel actions" - {
        "Async actions should be run in parallel" {
            val saga = saga<Int, Nothing> {
                val pool = Executors.newFixedThreadPool(3)
                val v1 = asyncAtomic({ pool.delayed(0.1.seconds) { 1 } }, compensateWith = {})
                val v2 = asyncAtomic({ pool.delayed(0.1.seconds) { 2 } }, compensateWith = {})
                val v3 = asyncAtomic({ pool.delayed(0.1.seconds) { 3 } }, compensateWith = {})
                result { listOf(v1.value, v2.value, v3.value).sum() }
            }

            val (result, duration) = measureTimedValue { saga.execute() }

            result.shouldBeInstanceOf<Saga.Result.Success<Int>>()
                .value shouldBe 6
            duration.shouldNotBeGreaterThan(0.15.seconds)
        }

        "Only completely finished actions are compensated" {
            val rollback = RollbackRecorder<Int>()
            val result = saga<Int, String> {
                val pool = Executors.newFixedThreadPool(3)
                val v1 = asyncAtomic({
                    pool.delayed(0.1.seconds) { 1 }
                }, compensateWith = rollback)
                val v2 = asyncAtomic({
                    pool.delayed(0.2.seconds) { 2 }
                }, compensateWith = rollback)
                val v3 = asyncAtomic({
                    pool.delayed(0.15.seconds) { fail("Test") }
                }, compensateWith = rollback)
                result { listOf(v1.value, v2.value, v3.value).sum() }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                .reason shouldBe "Test"
            rollback.shouldBeCalledWith(1)
        }
    }
})

private class RollbackRecorder<T : Any> private constructor(
    private val rollback: SagaActionRollbackScope.(T) -> Unit,
    private val argType: KClass<T>,
) : (SagaActionRollbackScope, T) -> Unit by rollback {
    fun shouldBeCalledWith(vararg args: T) {
        verifySequence {
            args.forEach { rollback.invoke(any(), it) }
        }
    }

    fun shouldNotBeCalled() {
        "Rollback should not be called".asClue {
            verify(exactly = 0) { rollback.invoke(any(), any(argType)) }
        }
    }

    companion object {
        inline operator fun <reified T : Any> invoke(): RollbackRecorder<T> =
            RollbackRecorder(
                rollback = spyk<SagaActionRollbackScope.(T) -> Unit>(),
                argType = T::class,
            )
    }
}

private fun <T> ExecutorService.delayed(
    sleep: Duration,
    preDelay: () -> Unit = {},
    f: () -> T
): Future<T> =
    submit<T> {
        preDelay()
        Thread.sleep(sleep.inWholeMilliseconds)
        f()
    }
