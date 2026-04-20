package main

import (
	"airbyte-service/core"
	postgresdatabase "airbyte-service/database/postgres"
	sourcecommon "airbyte-service/sync/sources/common"
	syncworker "airbyte-service/sync/worker"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var syncInterval time.Duration
var lastSyncEndTimeFilter time.Time
var mongoConnectionString string
var mongoDatabaseName string
var mongoTableName string
var postgresConnectionString string
var primaryKey string
var sources []sourcecommon.DatabaseScheme

func init() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, reading from environment")
	}

	mongoConnectionString = getEnv("MONGO_CONNECTION_STRING", "")
	mongoDatabaseName = getEnv("MONGO_DATABASE_NAME", "")
	mongoTableName = getEnv("MONGO_TABLE_NAME", "")
	postgresConnectionString = getEnv("POSTGRES_CONNECTION_STRING", "")
	primaryKey = getEnv("PRIMARY_KEY", "_id")

	if d, err := time.ParseDuration(getEnv("SYNC_INTERVAL", "1m")); err == nil {
		syncInterval = d
	} else {
		log.Fatalf("invalid SYNC_INTERVAL: %v", err)
	}

	if raw := getEnv("LAST_SYNC_END_TIME", ""); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			lastSyncEndTimeFilter = t
		} else {
			log.Fatalf("invalid LAST_SYNC_END_TIME (expected RFC3339): %v", err)
		}
	}

	fmt.Println(lastSyncEndTimeFilter)

	sources = buildSources()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	databasePostgres, err := postgresdatabase.NewDatabase(postgresdatabase.PostgresConfig{ConnectionString: postgresConnectionString})
	if err != nil {
		log.Fatalf("cannot connect to postgres: %v", err)
	}
	defer databasePostgres.Close()

	syncWorker := syncworker.New(
		syncInterval,
		databasePostgres.GetPool(),
		lastSyncEndTimeFilter,
		slog.Default(),
	)
	syncWorker.PutDatabasesWithTables(buildSources())

	if err := syncWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func buildSources() []sourcecommon.DatabaseScheme {
	return []sourcecommon.DatabaseScheme{
		{
			MongoURI: mongoConnectionString,
			Database: mongoDatabaseName,
			Tables: []*sourcecommon.Table{
				{
					Name:             mongoTableName,
					WriteMode:        core.WriteModeUpsert,
					PrimaryKey:       primaryKey,
					CreatedTimeField: "date",
					UpdatedTimeField: "updated_at",
					DeletedTimeField: "deleted_at",
					Fields: []sourcecommon.Field{
						{Name: "id", DestType: "VARCHAR", IsPrimary: true},
						{Name: "account.id", DestType: "VARCHAR"},
						{Name: "account.name", DestType: "VARCHAR"},
						{Name: "account.username", DestType: "VARCHAR"},
						{Name: "approved_at", DestType: "TIMESTAMPTZ"},
						{Name: "branch.id", DestType: "VARCHAR"},
						{Name: "branch.name", DestType: "VARCHAR"},
						{Name: "cash_box.id", DestType: "VARCHAR"},
						{Name: "cash_box.name", DestType: "VARCHAR"},
						{Name: "collected_at", DestType: "TIMESTAMPTZ"},
						{Name: "contractor.id", DestType: "VARCHAR"},
						{Name: "contractor.inn", DestType: "VARCHAR"},
						{Name: "contractor.name", DestType: "VARCHAR"},
						{Name: "created_at", DestType: "TIMESTAMPTZ"},
						{Name: "date", DestType: "TIMESTAMPTZ"},
						{Name: "deleted_at", DestType: "TIMESTAMPTZ"},
						{Name: "delivery_info", DestType: "VARCHAR"},
						{Name: "employee.id", DestType: "BIGINT"},
						{Name: "employee.name", DestType: "VARCHAR"},
						{Name: "handed_over_at", DestType: "TIMESTAMPTZ"},
						{Name: "is_approved", DestType: "BOOLEAN"},
						{Name: "is_collected", DestType: "BOOLEAN"},
						{Name: "is_deleted", DestType: "BOOLEAN"},
						{Name: "is_fiscalized", DestType: "BOOLEAN"},
						{Name: "is_for_debt", DestType: "BOOLEAN"},
						{Name: "is_handed_over", DestType: "BOOLEAN"},
						{Name: "is_reviewed", DestType: "BOOLEAN"},
						{Name: "number", DestType: "VARCHAR"},
						{Name: "location", DestType: "VARCHAR"},
						{Name: "organization.id", DestType: "VARCHAR"},
						{Name: "organization.name", DestType: "VARCHAR"},
						{Name: "payment.cash_box_states", DestType: "JSONB"},
						{Name: "payment.date", DestType: "TIMESTAMPTZ"},
						{Name: "payment.debt_states", DestType: "JSONB"},
						{Name: "payment.id", DestType: "VARCHAR"},
						{Name: "payment.notes", DestType: "VARCHAR"},
						{Name: "percent_discount", DestType: "DOUBLE PRECISION"},
						{Name: "reviewed_at", DestType: "TIMESTAMPTZ"},
						{Name: "status", DestType: "INTEGER"},
						{Name: "tags", DestType: "JSONB"},
						{Name: "updated_at", DestType: "TIMESTAMPTZ"},
						{Name: "exact_discounts", DestType: "JSONB"},
						// {Name: "exact_discounts.amount", DestType: "DOUBLE PRECISION"},
						// {Name: "exact_discounts.currency.id", DestType: "BIGINT"},
						// {Name: "exact_discounts.currency.is_national", DestType: "BOOLEAN"},
						// {Name: "exact_discounts.currency.name", DestType: "VARCHAR"},
						{Name: "net_price", DestType: "JSONB"},
						// {Name: "net_price.amount", DestType: "DOUBLE PRECISION"},
						// {Name: "net_price.currency.id", DestType: "BIGINT"},
						// {Name: "net_price.currency.is_national", DestType: "BOOLEAN"},
						// {Name: "net_price.currency.name", DestType: "VARCHAR"},

						{Name: "items", DestType: "JSONB", SeparateTable: true},
						{Name: "items.created_at", DestType: "TIMESTAMPTZ"},
						{Name: "items.deleted_at", DestType: "TIMESTAMPTZ"},
						{Name: "items.discount.amount", DestType: "DOUBLE PRECISION"},
						{Name: "items.discount.type", DestType: "INTEGER"},
						{Name: "items.discount.value", DestType: "DOUBLE PRECISION"},
						{Name: "items.id", DestType: "VARCHAR", IsPrimary: true},
						{Name: "items.is_deleted", DestType: "BOOLEAN"},
						{Name: "items.net_price.amount", DestType: "DOUBLE PRECISION"},
						{Name: "items.net_price.currency.id", DestType: "BIGINT"},
						{Name: "items.net_price.currency.is_national", DestType: "BOOLEAN"},
						{Name: "items.net_price.currency.name", DestType: "VARCHAR"},
						{Name: "items.original_price.amount", DestType: "DOUBLE PRECISION"},
						{Name: "items.original_price.currency.id", DestType: "BIGINT"},
						{Name: "items.original_price.currency.is_national", DestType: "BOOLEAN"},
						{Name: "items.original_price.currency.name", DestType: "VARCHAR"},
						{Name: "items.price.amount", DestType: "DOUBLE PRECISION"},
						{Name: "items.price.currency.id", DestType: "BIGINT"},
						{Name: "items.price.currency.is_national", DestType: "BOOLEAN"},
						{Name: "items.price.currency.name", DestType: "VARCHAR"},
						{Name: "items.quantity", DestType: "DOUBLE PRECISION"},
						{Name: "items.updated_at", DestType: "TIMESTAMPTZ"},
						{Name: "items.warehouse_item.id", DestType: "VARCHAR"},
						{Name: "items.warehouse_item.marks", DestType: "JSONB"},
						{Name: "items.warehouse_item.name", DestType: "VARCHAR"},
						{Name: "items.warehouse_item.warehouse.id", DestType: "INTEGER"},
						{Name: "items.warehouse_item.warehouse.name", DestType: "VARCHAR"},
						{Name: "items.warehouse_item.warehouse_item_use.after_quantity", DestType: "DOUBLE PRECISION"},
						{Name: "items.warehouse_item.warehouse_item_use.before_quantity", DestType: "DOUBLE PRECISION"},
						{Name: "items.warehouse_item.warehouse_item_use.id", DestType: "BIGINT"},
					},
				},
			},
		},
	}
}
