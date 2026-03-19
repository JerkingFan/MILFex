# MILFex

## Требования

- Go `1.26.x` (проверено на `darwin/arm64`)
- при сборке/запуске нужен флаг линковщика:

```bash
-ldflags='-checklinkname=0'
```

Без этого флага код с `goroutines.go` падает с ошибкой:
`invalid reference to runtime.newproc`.

## Структура проекта

- `goroutines.go` - API запуска задач (`Go`, `GoSafe`, `Group`)
- `milfex.go`, `lock.go`, `lockfree.go` - мьютекс и lock-free очередь
- `shredder/` - пакет планировщика
- `cmd/demo/main.go` - демонстрационный запуск

## Запуск демо

Из корня репозитория:

```bash
go run -ldflags='-checklinkname=0' ./cmd/demo
```

## Запуск тестов

Все тесты:

```bash
go test ./... -ldflags='-checklinkname=0'
```

Только `shredder`:

```bash
go test -v ./shredder -ldflags='-checklinkname=0'
```

Проверка гонок:

```bash
go test -race -count=100 -ldflags='-checklinkname=0'
```
