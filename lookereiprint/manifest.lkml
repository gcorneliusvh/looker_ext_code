
project_name: "lookereiprint"

application: lookereiprint {
  label: "lookereiprint"
  url: "https://localhost:8080/bundle.js"
  # file: "bundle.js
  entitlements: {
    core_api_methods: ["me"] #Add more entitlements here as you develop new functionality
  }
}
