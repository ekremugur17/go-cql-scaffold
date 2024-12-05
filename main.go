package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/gocql/gocql"
	"github.com/iancoleman/strcase"
)

func fetchTableNames(session *gocql.Session, keyspace string) ([]string, error) {
	var tableName string
	var tableNames []string

	query := fmt.Sprintf("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s'", keyspace)
	iter := session.Query(query).Iter()

	for iter.Scan(&tableName) {
		tableNames = append(tableNames, tableName)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func fetchColumnDefinitions(session *gocql.Session, keyspace string, tableName string) (map[string]string, error) {
	query := fmt.Sprintf("SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, tableName)
	iter := session.Query(query).Iter()

	var columnName string
	var columnType string
	columns := make(map[string]string)

	for iter.Scan(&columnName, &columnType) {
		columns[columnName] = columnType
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return columns, nil
}

func connectToScylla(host string, port int) (*gocql.Session, error) {
	cluster := gocql.NewCluster(host)
	cluster.Port = port
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return session, nil
}

func generateGoStruct(tableName string, columns map[string]string) (string, error) {
	structDefinition := fmt.Sprintf("type %s struct {\n", toPascal(tableName))

	for column, cqlType := range columns {
		goType, err := cqlToGoType(cqlType)

		if err != nil {
			return "", err
		}

		structDefinition += fmt.Sprintf("    %s %s `json:\"%s\"`\n", strcase.ToCamel(column), goType, column)
	}

	structDefinition += "}\n"
	return structDefinition, nil
}

func cqlToGoType(cqlType string) (string, error) {
	cqlType = strings.ToLower(strings.TrimSpace(cqlType))
	mapPattern := regexp.MustCompile(`^map<(.+),(.+)>$`)
	listPattern := regexp.MustCompile(`^list<(.+)>$`)
	setPattern := regexp.MustCompile(`^set<(.+)>$`)

	if matches := mapPattern.FindStringSubmatch(cqlType); matches != nil {
		keyType, valueType := matches[1], matches[2]

		goKeyType, err := cqlToGoType(keyType)
		if err != nil {
			return "", err
		}
		goValueType, err := cqlToGoType(valueType)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("map[%s]%s", goKeyType, goValueType), nil
	}

	if matches := listPattern.FindStringSubmatch(cqlType); matches != nil {
		elemType := matches[1]

		goElemType, err := cqlToGoType(elemType)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("[]%s", goElemType), nil
	}

	if matches := setPattern.FindStringSubmatch(cqlType); matches != nil {
		elemType := matches[1]

		goElemType, err := cqlToGoType(elemType)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("map[%s]struct{}", goElemType), nil
	}

	switch cqlType {
	case "uuid", "time.uuid":
		return "gocql.UUID", nil
	case "boolean":
		return "bool", nil
	case "text", "varchar":
		return "string", nil
	case "int":
		return "int", nil
	case "bigint":
		return "int64", nil
	case "tinyint":
		return "int8", nil
	case "smallint":
		return "int16", nil
	case "float":
		return "float32", nil
	case "double":
		return "float64", nil
	case "decimal":
		return "gocql.Decimal", nil
	case "timestamp":
		return "time.Time", nil
	case "date":
		return "gocql.Date", nil
	case "time":
		return "gocql.Time", nil
	case "blob":
		return "[]byte", nil
	default:
		return "", fmt.Errorf("unknown CQL type: %s", cqlType)
	}
}

func toPascal(value string) string {
	camel := strcase.ToCamel(value)
	return string(unicode.ToUpper(rune(camel[0]))) + camel[1:]
}

func main() {
	var host string
	var port int
	var keyspace string
	var outputDirectory string

	flag.StringVar(&host, "host", "localhost", "ScyllaDB host address")
	flag.IntVar(&port, "port", 9042, "ScyllaDB port")
	flag.StringVar(&keyspace, "keyspace", "", "Keyspace name")
	flag.StringVar(&outputDirectory, "outputDir", "./outputs", "Relative path to output directory")

	flag.Parse()

	if keyspace == "" {
		log.Fatal("Keyspace name is required")
	}

	session, err := connectToScylla(host, port)
	if err != nil {
		log.Fatalf("Could not connect to ScyllaDB: %v", err)
	}
	defer session.Close()

	tableNames, err := fetchTableNames(session, keyspace)
	if err != nil {
		log.Fatalf("Error fetching table definitions: %v", err)
	}

	var structDefinitions []string
	for _, tableName := range tableNames {
		columns, err := fetchColumnDefinitions(session, keyspace, tableName)
		if err != nil {
			log.Printf("Error fetching column definitions for table %s: %v", tableName, err)
			continue
		}

		structDef, err := generateGoStruct(tableName, columns)

		if err != nil {
			panic(err)
		}

		structDefinitions = append(structDefinitions, structDef)
	}

	dirPath := outputDirectory + "/" + keyspace
	filePath := dirPath + "/main.go"

	err = os.MkdirAll(dirPath, os.ModePerm)

	if err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		panic(err)
	}

	defer file.Close()

	file.WriteString("package main\n")

	for _, structDefinition := range structDefinitions {
		_, err = file.WriteString(structDefinition)

		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}

	fmt.Println("String written to file successfully.")
}
