package main

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestRunPoolFullResult(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8}
	want := []Result{
		{1, 1}, {2, 4}, {3, 9}, {4, 16},
		{5, 25}, {6, 36}, {7, 49}, {8, 64},
	}

	got, err := runPool(context.Background(), inputs, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("получено %v, ожидалось %v", got, want)
	}
}

func TestRunPoolCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := runPool(ctx, []int{1, 2, 3}, 4)
	if !errors.Is(err, context.Canceled) || len(got) != 0 {
		t.Fatalf("получено %v и %v, ожидался пустой результат с context.Canceled", got, err)
	}
}

func TestRunPoolInvalidWorkers(t *testing.T) {
	_, err := runPool(context.Background(), []int{1}, 0)
	if err == nil {
		t.Fatal("ожидалась ошибка при нуле обработчиков")
	}
}
