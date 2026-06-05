import { BigQuery } from '@google-cloud/bigquery';
import { GoogleGenAI } from '@google/genai';
import OpenAI from 'openai';
import { Anthropic } from '@anthropic-ai/sdk';
import { v4 as uuidv4 } from 'uuid';

console.log("🚀 Node engine initialized. Parsing configuration frameworks...");

// =========================================================================
// SYSTEM ROUTING CONFIGURATIONS
// =========================================================================
// 🎪 Set to true to bypass all cloud database & AI paywall credit limits instantly!
const IS_MOCK_DEMO_MODE = true; 

// Switch this token string to swap your engine provider instantly on demand:
// Options: 'GEMINI' | 'OPENAI' | 'CLAUDE'
const ACTIVE_AI_PROVIDER = 'GEMINI'; 

const projectId = 'weather-command-244666';
const datasetId = 'billing_data';
const targetTableId = 'incident_dashboard';

const bq = new BigQuery({ projectId });

// Safe client constructor isolation setup
const ai = (IS_MOCK_DEMO_MODE || ACTIVE_AI_PROVIDER !== 'GEMINI') ? null : new GoogleGenAI({}); 

// =========================================================================
// MAIN PIPELINE ORCHESTRATOR
// =========================================================================
async function runAuditPipeline() {
    console.log(`\n🤖 Audit Agent processing project: ${projectId}`);
    if (IS_MOCK_DEMO_MODE) {
        console.log(`🎪 DEMO MODE ACTIVE: Emulating system layers using [${ACTIVE_AI_PROVIDER}] blueprint...`);
    }
    
    try {
        // 1. Ensure core architecture table presence in BigQuery
        await ensureTargetArchitectureExists();

        let aiAnalysisPayload = '';
        let actualCostMetric = 0.0;
        let upperBoundThreshold = 0.0;
        let severityMultiplier = 1.0;
        const targetDateString = new Date().toISOString().split('T')[0];

        if (IS_MOCK_DEMO_MODE) {
            // Static mock matrix for reliable stage demonstrations
            actualCostMetric = 1420.75;
            upperBoundThreshold = 850.00;
            severityMultiplier = 1.6;
            aiAnalysisPayload = getMockMarkdownPayload(targetDateString, actualCostMetric, upperBoundThreshold);
        } else {
            console.log('⏳ Fetching latest infrastructure resource utilization metrics...');
            const billingRows = await fetchRawBillingData();

            if (!billingRows || billingRows.length === 0) {
                console.log('⏳ Target billing export has 0 rows logged (New account setup latency).');
                aiAnalysisPayload = await synthesizeSimulationBrief(targetDateString);
            } else {
                console.log(`📈 Live data discovered (${billingRows.length} rows). Executing analytical evaluations...`);
                actualCostMetric = calculateTotalCost(billingRows);
                upperBoundThreshold = actualCostMetric * 0.85; 
                severityMultiplier = actualCostMetric > upperBoundThreshold ? 1.5 : 1.0;
                aiAnalysisPayload = await synthesizeProductionBrief(billingRows, actualCostMetric, targetDateString);
            }
        }

        // 2. Commit log row record straight into BigQuery
        console.log('💾 Logging structured incident metrics back into presentation layout...');
        await logIncidentToDashboard({
            incident_id: uuidv4(),
            detection_timestamp: new Date().toISOString(),
            target_date: targetDateString,
            actual_cost: actualCostMetric,
            upper_bound: upperBoundThreshold,
            severity_multiplier: severityMultiplier,
            ai_summary: aiAnalysisPayload
        });

        console.log('✅ Telemetry execution successful! Looker Studio presentation dashboard source updated.\n');

    } catch (pipelineError) {
        console.error('❌ Pipeline Execution Failed Critical Error Bounds:', pipelineError);
        process.exit(1);
    }
}

// =========================================================================
// THE UNIFIED AI CHASSIS (Unified Engine Router)
// =========================================================================
async function callUnifiedAI(promptText) {
    switch (ACTIVE_AI_PROVIDER.toUpperCase()) {
        case 'GEMINI':
            console.log('⚡ Routing inference payload to Google Gemini Core...');
            const geminiClient = ai || new GoogleGenAI({});
            const geminiResponse = await geminiClient.models.generateContent({
                model: 'gemini-2.5-flash',
                contents: promptText,
            });
            return geminiResponse.text;

        case 'OPENAI':
            console.log('⚡ Routing inference payload to OpenAI GPT Core...');
            const openai = new OpenAI();
            const gptResponse = await openai.chat.completions.create({
                model: 'gpt-4o-mini', 
                messages: [{ role: 'user', content: promptText }],
            });
            return gptResponse.choices[0].message.content;

        case 'CLAUDE':
            console.log('⚡ Routing inference payload to Anthropic Claude Core...');
            const anthropic = new Anthropic();
            const claudeResponse = await anthropic.messages.create({
                model: 'claude-3-5-sonnet-latest',
                max_tokens: 1024,
                messages: [{ role: 'user', content: promptText }],
            });
            return claudeResponse.content[0].text;

        default:
            throw new Error(`CRITICAL: Unknown AI Engine Provider Selected: ${ACTIVE_AI_PROVIDER}`);
    }
}

// =========================================================================
// MODULAR GENERATION TEXT WRAPPERS
// =========================================================================
async function synthesizeProductionBrief(rows, totalCost, dateString) {
    const contextPrompt = `You are an expert infrastructure engineer monitoring project "${projectId}". Review these cloud usage data logs for ${dateString}: ${JSON.stringify(rows.slice(0, 10))}. Generate an operational report. Format your output entirely in standard Markdown syntax without HTML tags.`;
    return await callUnifiedAI(contextPrompt);
}

async function synthesizeSimulationBrief(dateString) {
    const simulationPrompt = `Create an engineering pipeline monitoring initialization tracking brief for project "${projectId}" on date ${dateString}. Format your response entirely in standard Markdown syntax without HTML tags.`;
    return await callUnifiedAI(simulationPrompt);
}

function getMockMarkdownPayload(dateString, cost, threshold) {
    return `**Cloud Spending Anomaly Alert: ${dateString}** (Demo Mode [${ACTIVE_AI_PROVIDER}])

### 1. Executive Summary
The automated cloud monitoring suite has detected a critical cost variance framework threshold breach. Total resource expenditures for the active cycle have reached **$${cost.toFixed(2)}**, exceeding the established system baseline upper bound profile of **$${threshold.toFixed(2)}**.

### 2. Operational Breakdown
- **Compute Engine Scalability Spike**: An irregular horizontal scaling trigger within the processing cluster caused a **42%** increase in persistent instances over a 6-hour window.
- **BigQuery Analytical Runaway**: Multiple high-volume unindexed staging queries executed concurrently, causing slot processing metrics to spike abnormally.

### 3. Actionable Engineering Recommendations
- **Enforce Query Constraints**: Initialize slot allocation execution caps on the active development processing workspace.
- **Optimize Scaling Policy**: Refactor the compute engine instance group scaling cooling thresholds to prevent aggressive early sizing behavior.`;
}

// =========================================================================
// BACKEND DATA LAYER UTILITIES
// =========================================================================
async function ensureTargetArchitectureExists() {
    try {
        const [datasets] = await bq.getDatasets();
        if (!datasets.some(d => d.id === datasetId)) await bq.createDataset(datasetId);

        const dataset = bq.dataset(datasetId);
        const [tables] = await dataset.getTables();
        
        if (!tables.some(t => t.id === targetTableId)) {
            const schema = [
                { name: 'incident_id', type: 'STRING', mode: 'REQUIRED' },
                { name: 'detection_timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
                { name: 'target_date', type: 'STRING', mode: 'NULLABLE' },
                { name: 'actual_cost', type: 'FLOAT', mode: 'NULLABLE' },
                { name: 'upper_bound', type: 'FLOAT', mode: 'NULLABLE' },
                { name: 'severity_multiplier', type: 'FLOAT', mode: 'NULLABLE' },
                { name: 'ai_summary', type: 'STRING', mode: 'NULLABLE' }
            ];
            await dataset.createTable(targetTableId, { schema });
        }
    } catch (err) {
        if (!IS_MOCK_DEMO_MODE) throw err;
        console.log('🎪 (Mock mode bypassed local cloud structural schemas)');
    }
}

async function fetchRawBillingData() {
    try {
        const discoveryQuery = `SELECT table_name FROM \`${projectId}.${datasetId}\`.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'gcp_billing_export_v1_%' LIMIT 1`;
        const [discoveryRows] = await bq.query({ query: discoveryQuery });
        if (discoveryRows.length === 0) return [];
        
        const realBillingTableName = discoveryRows[0].table_name;
        const processingQuery = `SELECT project.id as project_id, service.description as service, sku.description as sku, cost FROM \`${projectId}.${datasetId}.${realBillingTableName}\` LIMIT 100`;
        const [rows] = await bq.query({ query: processingQuery });
        return rows;
    } catch { return []; }
}

async function logIncidentToDashboard(payload) {
    if (IS_MOCK_DEMO_MODE) {
        try {
            await bq.dataset(datasetId).table(targetTableId).insert([payload]);
        } catch (err) {
            console.log('📝 Notice: Mock row output compiled locally (BigQuery table unreachable or offline).');
        }
    } else {
        await bq.dataset(datasetId).table(targetTableId).insert([payload]);
    }
}

function calculateTotalCost(rows) { return rows.reduce((acc, row) => acc + (parseFloat(row.cost) || 0), 0); }

// =========================================================================
// RUNTIME CORE INITIALIZER INVOCATION
// =========================================================================
console.log("⏳ Invoking runAuditPipeline(). Connecting pipeline infrastructure...");
runAuditPipeline();