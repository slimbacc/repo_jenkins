o pipeline {
    agent any
    
    stages {
        stage('Checkout') {
            steps {
                // Checkout the code from your Git repository
                checkout scm
            }
        }
        stage('Build') {
            steps {
                // Run the Python script
                sh 'python process.py'
            }
        }
        // Add more stages as needed
    }
    
    post {
        success {
            echo 'success'
        }
        failure {
            echo 'failure'
        }
    }
}
