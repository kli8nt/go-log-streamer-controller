package pkg

import (
	"log"
	"os"

	_ "github.com/joho/godotenv/autoload"
)

func init() {
	log.Printf("Starting with WITH_KUBECONFIG set to: %s\n", os.Getenv("WITH_KUBECONFIG"))
	log.Printf("Starting with NAMESPACE set to: %s\n", os.Getenv("NAMESPACE"))
}