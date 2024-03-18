package io.dwsoft.sok

import io.kotest.assertions.asClue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.assertions.throwables.shouldThrowAnyUnit
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.comparables.shouldNotBeGreaterThan
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifySequence
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTimedValue

// TODO: maybe group test cases by execution and rollback phases
class NonSuspendingSagaTest : FreeSpec({
    "Action results are usable in..." - {
        "...other actions" {
            val recorder = Recorder<Int>()
            saga<Unit, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                atomic({ recorder(v.value) }, compensateWith = {})
                result {}
            }.execute()

            verify(exactly = 1) { recorder(1) }
        }

        "...rollbacks" {
            val recorder = Recorder<Int>()
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
            val recorder = Recorder<Int>()
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
    //  * what happens when failed action cancels another and the later fails during this -
    //  does cancel() returns false? Is rollback of such action skipped (as it should)?
    "Parallel actions" - {
        "Async actions should be run in parallel" {
            val saga = saga<Int, Nothing> {
                val pool = Executors.newFixedThreadPool(3)
                val v1 = atomic(asynchronous({ pool.delayed(0.1.seconds) { 1 }.asAsync() }, compensateWith = {}))
                val v2 = atomic(asynchronous({ pool.delayed(0.1.seconds) { 2 }.asAsync() }, compensateWith = {}))
                val v3 = atomic(asynchronous({ pool.delayed(0.1.seconds) { 3 }.asAsync() }, compensateWith = {}))
                result { listOf(v1.value, v2.value, v3.value).sum() }
            }

            val (result, duration) = measureTimedValue { saga.execute() }

            result.shouldBeInstanceOf<Saga.Result.Success<Int>>()
                .value shouldBe 6
            duration.shouldNotBeGreaterThan(0.15.seconds)
        }

        "Nested sagas are run synchronously" {
            val recorder = Recorder<Int>()
            val saga = saga<Unit, Nothing> {
                atomic(asynchronous({ delayed(0.3.seconds) { recorder(4) }.asAsync() }, compensateWith = {}))
                atomic(
                    saga {
                        atomic(asynchronous({ delayed(0.2.seconds) { recorder(1) }.asAsync() }, compensateWith = {}))
                        atomic(asynchronous({ delayed(0.21.seconds) { recorder(2) }.asAsync() }, compensateWith = {}))
                        result {}
                    }
                )
                atomic(asynchronous({ delayed(0.05.seconds) { recorder(3) }.asAsync() }, compensateWith = {}))
                result {}
            }

            val (result, duration) = measureTimedValue {
                saga.execute()
            }

            result.shouldBeInstanceOf<Saga.Result.Success<Unit>>()
            recorder.shouldBeCalledWith(1, 2, 3, 4)
            duration shouldBeBetween 0.3.seconds..0.35.seconds
        }

        "Only completely finished actions are compensated" {
            val rollback = RollbackRecorder<Int>()
            val pool = Executors.newFixedThreadPool(3)
            var cancelledFuture: Future<Int>? = null

            val result = saga<Int, String> {
                val v1 = atomic(asynchronous({
                    pool.delayed(0.1.seconds) { 1 }.asAsync()
                }, compensateWith = rollback))
                val v2 = atomic(asynchronous({
                    pool.delayed(0.2.seconds) { 2 }.also { cancelledFuture = it }.asAsync()
                }, compensateWith = rollback))
                val v3 = atomic(asynchronous({
                    pool.delayed(0.15.seconds) { fail("Test") }.asAsync()
                }, compensateWith = rollback))
                result { listOf(v1.value, v2.value, v3.value).sum() }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                .reason shouldBe "Test"
            rollback.shouldBeCalledWith(1)
            cancelledFuture.shouldNotBeNull().isCancelled shouldBe true
        }

        "Partially completed nested sagas are compensated" {
            val rollback = RollbackRecorder<Int>()
            val unexpectedRollback = RollbackRecorder<Int>()
            var cancelledFuture: Future<Int>? = null

            val result = saga<Int, String> {
                atomic(asynchronous({ delayed(0.1.seconds) { 1 }.asAsync() }, compensateWith = rollback))
                val v = atomic(
                    saga {
                        atomic(asynchronous({ delayed(0.2.seconds) { 2 }.asAsync() }, compensateWith = rollback))
                        atomic(asynchronous({ delayed(0.21.seconds) { 3 }.asAsync() }, compensateWith = rollback))
                        atomic(asynchronous({ delayed(0.3.seconds) { 4 }.also { cancelledFuture = it }.asAsync() }, compensateWith = unexpectedRollback))
                        val v = atomic(asynchronous({ delayed(0.22.seconds) { fail("Test") }.asAsync() }, compensateWith = unexpectedRollback))
                        result { v.value }
                    }
                )
                result { v.value }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                .reason shouldBe "Test"
            rollback.shouldBeCalledWith(3, 2, 1)
            unexpectedRollback.shouldNotBeCalled()
            cancelledFuture.shouldNotBeNull().isCancelled shouldBe true
        }

        "Completed actions are not cancelled" {
            val unexpectedRollback = RollbackRecorder<Int>()
            var cancelledFuture: Future<out Int>? = null
            val saga = saga<Int, String> {
                atomic(asynchronous({ delayed(0.1.seconds) { fail("Test 1") }.asAsync() }, compensateWith = unexpectedRollback))
                atomic(asynchronous({ delayed(0.1.seconds) { fail("Test 2") }.also { cancelledFuture = it }.asAsync() }, compensateWith = unexpectedRollback))
                result { delayed(0.2.seconds) { 1 }.get() }
            }

            val result = saga.execute()

            result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                .reason shouldBe "Test 1"
            unexpectedRollback.shouldNotBeCalled()
            cancelledFuture.shouldNotBeNull().isCancelled shouldBe false
        }

        "Uncompleted actions are cancelled and saga fail fast when error is thrown from asynchronous action" {
            val expectedException = RuntimeException()
            val unexpectedRollback = RollbackRecorder<Int>()
            var cancelledFuture: Future<out Int>? = null
            val saga = saga<Int, String> {
                atomic(asynchronous({ delayed(0.3.seconds) { 1 }.also { cancelledFuture = it }.asAsync() }, compensateWith = unexpectedRollback))
                atomic(asynchronous({ delayed(0.1.seconds) { 2 }.asAsync() }, compensateWith = unexpectedRollback))
                atomic(asynchronous({ delayed(0.2.seconds) { throw expectedException }.asAsync() }, compensateWith = unexpectedRollback))
                result { delayed(0.3.seconds) { 1 }.get() }
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedException
            unexpectedRollback.shouldNotBeCalled()
            cancelledFuture.shouldNotBeNull().isCancelled shouldBe true
        }
    }
})

private open class Recorder<T : Any>(
    private val mock: (T) -> Unit,
    private val argType: KClass<T>
) : (T) -> Unit by mock {
    fun shouldBeCalledWith(vararg args: T) {
        verifySequence {
            args.forEach { mock.invoke(it) }
        }
    }

    fun shouldNotBeCalled() {
        "Function should not be called".asClue {
            verify(exactly = 0) { mock.invoke(any(argType)) }
        }
    }

    companion object {
        inline operator fun <reified T : Any> invoke(): Recorder<T> =
            Recorder(mock = spyk<(T) -> Unit>(), argType = T::class)
    }
}

private open class RollbackRecorder<T : Any> private constructor(
    private val rollback: SagaActionRollbackScope.(T) -> Unit,
    mock: (T) -> Unit,
    argType: KClass<T>,
) : Recorder<T>(mock, argType), (SagaActionRollbackScope, T) -> Unit by rollback {
    companion object {
        inline operator fun <reified T : Any> invoke(): RollbackRecorder<T> {
            val mock = spyk<(T) -> Unit>()
            return RollbackRecorder(rollback = { mock(it) }, mock = mock, argType = T::class)
        }
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

private fun <T> delayed(
    sleep: Duration,
    preDelay: () -> Unit = {},
    f: () -> T
): Future<T> =
    CompletableFuture.supplyAsync {
        preDelay()
        Thread.sleep(sleep.inWholeMilliseconds)
        f()
    }

private infix fun Duration.shouldBeBetween(range: ClosedRange<Duration>) {
    "Duration $this should be in range $range".asClue {
        (this in range) shouldBe true
    }
}
