// swift-tools-version:5.1
import PackageDescription

let package = Package(
	name: "CAsync",
	products: [
		.library(
			name: "CAsync",
			targets: ["CAsync"]
		),
	],
	targets: [
		.target(
			name: "CAsync",
			dependencies: []
		),
	]
)
