package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

type Result struct {
	Input  int
	Square int
}

func process(ctx context.Context, number int) (Result, error) {
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-timer.C:
		return Result{Input: number, Square: number * number}, nil
	}
}

func runSequential(ctx context.Context, inputs []int) ([]Result, error) {
	var results []Result
	for _, number := range inputs {
		result, err := process(ctx, number)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func runPool(ctx context.Context, inputs []int, workers int) ([]Result, error) {
	if workers < 1 {
		return nil, fmt.Errorf("число обработчиков должно быть больше нуля")
	}

	jobs := make(chan int)
	results := make(chan Result)
	senderDone := make(chan struct{})

	go func() {
		defer close(senderDone)
		defer close(jobs)

		for _, number := range inputs {
			select {
			case <-ctx.Done():
				return
			case jobs <- number:
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			for number := range jobs {
				result, err := process(ctx, number)
				if err != nil {
					return
				}

				select {
				case <-ctx.Done():
					return
				case results <- result:
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		<-senderDone
		close(results)
	}()

	var collected []Result
	for result := range results {
		collected = append(collected, result)
	}
	slices.SortFunc(collected, func(a, b Result) int {
		return cmp.Compare(a.Input, b.Input)
	})

	return collected, ctx.Err()
}

func runWithTimeout(inputs []int) ([]Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	return runPool(ctx, inputs, 4)
}

func show(name string, run func() ([]Result, error)) {
	start := time.Now()
	results, err := run()
	elapsed := time.Since(start)

	sum := 0
	for _, result := range results {
		sum += result.Square
	}
	fmt.Printf("%s: %d результатов, сумма %d, %v\n", name, len(results), sum, elapsed)
	fmt.Println("Пары {число квадрат}:", results)
	if err != nil {
		fmt.Println("Ошибка:", err)
	}
}

func main() {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8}
	ctx := context.Background()

	show("Последовательно", func() ([]Result, error) {
		return runSequential(ctx, inputs)
	})
	show("Пул из 1 обработчика", func() ([]Result, error) {
		return runPool(ctx, inputs, 1)
	})
	show("Пул из 4 обработчиков", func() ([]Result, error) {
		return runPool(ctx, inputs, 4)
	})
	show("Пул из 4 обработчиков, таймаут 300 мс", func() ([]Result, error) {
		return runWithTimeout(inputs)
	})
}
