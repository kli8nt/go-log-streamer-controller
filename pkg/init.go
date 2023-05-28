package pkg

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()
	log.Printf("Starting with WITH_KUBECONFIG set to: %s\n", os.Getenv("WITH_KUBECONFIG"))
	log.Printf("Starting with NAMESPACE set to: %s\n", os.Getenv("NAMESPACE"))
}
