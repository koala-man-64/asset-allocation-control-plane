targetScope = 'subscription'

@description('Email recipients for budget and anomaly notifications.')
param contactEmails array

@description('Primary notification email used for unsubscribe and service messages.')
param notificationEmail string

@description('UTC budget start date. Use the first day of the month, for example 2026-03-01T00:00:00Z.')
param startDate string

@description('UTC budget end date. Use a future date far enough out to keep recurring monthly budgets active.')
param endDate string

@description('Resource groups included in the budget filters.')
param resourceGroupFilterValues array

@description('Per-meter-category budget definitions.')
param budgetDefinitions array

@description('Enable the subscription-scoped daily anomaly insight alert.')
param anomalyAlertEnabled bool = true

@description('Scheduled action resource name for the anomaly alert.')
param anomalyAlertName string = 'asset-allocation-cost-anomaly'

@description('Display name for the anomaly alert.')
param anomalyAlertDisplayName string = 'Asset Allocation Cost Anomaly'

@description('Message body for the anomaly alert email.')
param anomalyAlertMessage string = 'Subscription-scoped cost anomaly alert for the Asset Allocation stack.'

@description('UTC hour of day for the anomaly alert delivery window.')
@minValue(0)
@maxValue(23)
param anomalyAlertHourOfDay int = 13

@description('Built-in Cost Management view used for the anomaly alert.')
param anomalyAlertViewName string = 'ms:DailyAnomalyByResourceGroup'

resource budgets 'Microsoft.Consumption/budgets@2023-11-01' = [
  for budget in budgetDefinitions: {
    name: string(budget.name)
    properties: {
      timePeriod: {
        startDate: startDate
        endDate: endDate
      }
      timeGrain: 'Monthly'
      amount: int(budget.amount)
      category: 'Cost'
      notifications: {
        ActualThreshold: {
          enabled: true
          operator: 'GreaterThan'
          threshold: int(budget.actualThreshold ?? 80)
          contactEmails: contactEmails
          contactRoles: []
          contactGroups: []
        }
        ForecastThreshold: {
          enabled: true
          operator: 'GreaterThan'
          threshold: int(budget.forecastThreshold ?? 100)
          thresholdType: 'Forecasted'
          contactEmails: contactEmails
          contactRoles: []
          contactGroups: []
        }
      }
      filter: {
        and: [
          {
            dimensions: {
              name: 'ResourceGroupName'
              operator: 'In'
              values: resourceGroupFilterValues
            }
          }
          {
            dimensions: {
              name: 'MeterCategory'
              operator: 'In'
              values: budget.meterCategories
            }
          }
        ]
      }
    }
  }
]

resource anomalyAlert 'Microsoft.CostManagement/scheduledActions@2025-03-01' = if (anomalyAlertEnabled) {
  name: anomalyAlertName
  kind: 'InsightAlert'
  properties: {
    displayName: anomalyAlertDisplayName
    notification: {
      message: anomalyAlertMessage
      subject: '${anomalyAlertDisplayName} detected'
      to: contactEmails
    }
    notificationEmail: notificationEmail
    schedule: {
      dayOfMonth: 0
      daysOfWeek: []
      endDate: endDate
      frequency: 'Daily'
      hourOfDay: anomalyAlertHourOfDay
      startDate: startDate
      weeksOfMonth: []
    }
    scope: subscription().id
    status: 'Enabled'
    viewId: resourceId('Microsoft.CostManagement/views', anomalyAlertViewName)
  }
}

output budgetIds array = [for (budget, index) in budgetDefinitions: budgets[index].id]
output anomalyAlertId string = anomalyAlertEnabled ? anomalyAlert.id : ''
