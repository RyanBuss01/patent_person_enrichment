# =============================================================================
# runners/monitor_report.py
# Step 5: Monitoring and Reporting
# =============================================================================
import json
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, Any, List
import os

logger = logging.getLogger(__name__)

class ReportGenerator:
    """Generate monitoring reports and analytics"""
    
    def __init__(self, output_dir: str = 'reports'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_pipeline_report(self, pipeline_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive pipeline performance report"""
        try:
            report_data = {
                'pipeline_summary': self._create_pipeline_summary(pipeline_results),
                'extraction_metrics': self._analyze_extraction_metrics(pipeline_results),
                'enrichment_metrics': self._analyze_enrichment_metrics(pipeline_results),
                'crm_integration_metrics': self._analyze_crm_metrics(pipeline_results),
                'email_metrics': self._analyze_email_metrics(pipeline_results),
                'recommendations': self._generate_recommendations(pipeline_results)
            }
            
            # Save detailed report
            report_file = os.path.join(self.output_dir, f'pipeline_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
            with open(report_file, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
            
            # Generate summary dashboard
            dashboard_file = self._create_dashboard(report_data)
            
            logger.info(f"Pipeline report generated: {report_file}")
            logger.info(f"Dashboard created: {dashboard_file}")
            
            return {
                'success': True,
                'report_file': report_file,
                'dashboard_file': dashboard_file,
                'summary': report_data['pipeline_summary']
            }
            
        except Exception as e:
            logger.error(f"Error generating pipeline report: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _create_pipeline_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Create high-level pipeline summary"""
        extraction_results = results.get('extraction', {})
        enrichment_results = results.get('enrichment', {})
        crm_results = results.get('crm_integration', {})
        email_results = results.get('email_automation', {})
        
        return {
            'run_timestamp': datetime.now().isoformat(),
            'extraction': {
                'patents_extracted': extraction_results.get('total_patents', 0),
                'success': extraction_results.get('success', False)
            },
            'enrichment': {
                'people_processed': enrichment_results.get('total_people', 0),
                'people_enriched': enrichment_results.get('enriched_count', 0),
                'enrichment_rate': enrichment_results.get('enrichment_rate', 0.0),
                'success': enrichment_results.get('success', False)
            },
            'crm_integration': {
                'leads_created': crm_results.get('leads_created', 0),
                'leads_failed': crm_results.get('leads_failed', 0),
                'success': crm_results.get('success', False)
            },
            'email_automation': {
                'emails_sent': email_results.get('emails_sent', 0),
                'emails_failed': email_results.get('emails_failed', 0),
                'success': email_results.get('success', False)
            }
        }
    
    def _analyze_extraction_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze patent extraction performance"""
        extraction_results = results.get('extraction', {})
        
        if not extraction_results.get('success'):
            return {'status': 'failed', 'error': extraction_results.get('error')}
        
        patents_data = extraction_results.get('patents_data', [])
        
        if not patents_data:
            return {'status': 'no_data'}
        
        # Analyze patent distribution
        patent_dates = [p.get('patent_date') for p in patents_data if p.get('patent_date')]
        assignee_orgs = [p.get('assignees', [{}])[0].get('assignee_organization') 
                        for p in patents_data if p.get('assignees')]
        
        return {
            'status': 'success',
            'total_patents': len(patents_data),
            'date_range': {
                'earliest': min(patent_dates) if patent_dates else None,
                'latest': max(patent_dates) if patent_dates else None
            },
            'top_assignees': self._get_top_items(assignee_orgs, 10),
            'patents_with_inventors': len([p for p in patents_data if p.get('inventors')]),
            'patents_with_assignees': len([p for p in patents_data if p.get('assignees')])
        }
    
    def _analyze_enrichment_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze enrichment performance"""
        enrichment_results = results.get('enrichment', {})
        
        if not enrichment_results.get('success'):
            return {'status': 'failed', 'error': enrichment_results.get('error')}
        
        enriched_data = enrichment_results.get('enriched_data', [])
        
        if not enriched_data:
            return {'status': 'no_data'}
        
        # Analyze enrichment quality
        match_scores = [person.get('match_score', 0) for person in enriched_data]
        api_methods = [person.get('enriched_data', {}).get('api_method') for person in enriched_data]
        person_types = [person.get('enriched_data', {}).get('person_type') for person in enriched_data]
        
        # Count people with email addresses
        people_with_emails = len([
            person for person in enriched_data 
            if person.get('enriched_data', {}).get('pdl_data', {}).get('emails')
        ])
        
        return {
            'status': 'success',
            'total_enriched': len(enriched_data),
            'people_with_emails': people_with_emails,
            'email_rate': (people_with_emails / len(enriched_data) * 100) if enriched_data else 0,
            'match_score_stats': {
                'average': sum(match_scores) / len(match_scores) if match_scores else 0,
                'high_confidence': len([s for s in match_scores if s >= 0.8]),
                'medium_confidence': len([s for s in match_scores if 0.5 <= s < 0.8]),
                'low_confidence': len([s for s in match_scores if s < 0.5])
            },
            'api_method_distribution': self._count_items(api_methods),
            'person_type_distribution': self._count_items(person_types)
        }
    
    def _analyze_crm_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze CRM integration performance"""
        crm_results = results.get('crm_integration', {})
        
        if not crm_results.get('success'):
            return {'status': 'failed', 'error': crm_results.get('error')}
        
        leads_created = crm_results.get('leads_created', 0)
        leads_failed = crm_results.get('leads_failed', 0)
        total_processed = crm_results.get('total_processed', 0)
        
        success_rate = (leads_created / total_processed * 100) if total_processed > 0 else 0
        
        return {
            'status': 'success',
            'leads_created': leads_created,
            'leads_failed': leads_failed,
            'total_processed': total_processed,
            'success_rate': success_rate,
            'failure_rate': (leads_failed / total_processed * 100) if total_processed > 0 else 0
        }
    
    def _analyze_email_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze email automation performance"""
        email_results = results.get('email_automation', {})
        
        if not email_results.get('success'):
            return {'status': 'failed', 'error': email_results.get('error')}
        
        emails_sent = email_results.get('emails_sent', 0)
        emails_failed = email_results.get('emails_failed', 0)
        no_email = email_results.get('no_email_address', 0)
        total_processed = email_results.get('total_processed', 0)
        
        send_rate = (emails_sent / total_processed * 100) if total_processed > 0 else 0
        
        return {
            'status': 'success',
            'emails_sent': emails_sent,
            'emails_failed': emails_failed,
            'no_email_address': no_email,
            'total_processed': total_processed,
            'send_rate': send_rate,
            'deliverability_rate': (emails_sent / (emails_sent + emails_failed) * 100) if (emails_sent + emails_failed) > 0 else 0
        }
    
    def _generate_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on pipeline performance"""
        recommendations = []
        
        # Enrichment recommendations
        enrichment_metrics = self._analyze_enrichment_metrics(results)
        if enrichment_metrics.get('status') == 'success':
            email_rate = enrichment_metrics.get('email_rate', 0)
            if email_rate < 50:
                recommendations.append(f"Email discovery rate is low ({email_rate:.1f}%). Consider using additional data sources or enrichment providers.")
            
            avg_match_score = enrichment_metrics.get('match_score_stats', {}).get('average', 0)
            if avg_match_score < 0.6:
                recommendations.append(f"Average match score is low ({avg_match_score:.2f}). Review enrichment parameters and consider stricter filtering.")
        
        # CRM integration recommendations
        crm_metrics = self._analyze_crm_metrics(results)
        if crm_metrics.get('status') == 'success':
            success_rate = crm_metrics.get('success_rate', 0)
            if success_rate < 90:
                recommendations.append(f"CRM integration success rate is {success_rate:.1f}%. Check for data validation issues or API limits.")
        
        # Email recommendations
        email_metrics = self._analyze_email_metrics(results)
        if email_metrics.get('status') == 'success':
            send_rate = email_metrics.get('send_rate', 0)
            if send_rate < 30:
                recommendations.append(f"Email send rate is low ({send_rate:.1f}%). Focus on improving email discovery in enrichment phase.")
        
        return recommendations
    
    def _create_dashboard(self, report_data: Dict[str, Any]) -> str:
        """Create visual dashboard"""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('Patent Processing Pipeline Dashboard', fontsize=16)
            
            # Pipeline summary
            summary = report_data['pipeline_summary']
            pipeline_stages = ['Patents\nExtracted', 'People\nEnriched', 'Leads\nCreated', 'Emails\nSent']
            pipeline_values = [
                summary['extraction']['patents_extracted'],
                summary['enrichment']['people_enriched'],
                summary['crm_integration']['leads_created'],
                summary['email_automation']['emails_sent']
            ]
            
            axes[0, 0].bar(pipeline_stages, pipeline_values, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
            axes[0, 0].set_title('Pipeline Throughput')
            axes[0, 0].set_ylabel('Count')
            
            # Enrichment quality
            enrichment_metrics = report_data.get('enrichment_metrics', {})
            if enrichment_metrics.get('status') == 'success':
                match_stats = enrichment_metrics.get('match_score_stats', {})
                confidence_labels = ['High\n(≥0.8)', 'Medium\n(0.5-0.8)', 'Low\n(<0.5)']
                confidence_values = [
                    match_stats.get('high_confidence', 0),
                    match_stats.get('medium_confidence', 0),
                    match_stats.get('low_confidence', 0)
                ]
                
                axes[0, 1].pie(confidence_values, labels=confidence_labels, autopct='%1.1f%%')
                axes[0, 1].set_title('Match Score Distribution')
            
            # Success rates
            rates = {
                'Enrichment': summary['enrichment'].get('enrichment_rate', 0),
                'CRM Success': crm_metrics.get('success_rate', 0) if 'crm_metrics' in locals() else 0,
                'Email Send': email_metrics.get('send_rate', 0) if 'email_metrics' in locals() else 0
            }
            
            axes[1, 0].bar(rates.keys(), rates.values(), color=['#ff7f0e', '#2ca02c', '#d62728'])
            axes[1, 0].set_title('Success Rates (%)')
            axes[1, 0].set_ylabel('Percentage')
            axes[1, 0].set_ylim(0, 100)
            
            # Timeline placeholder (would need historical data)
            axes[1, 1].text(0.5, 0.5, 'Historical Trends\n(Requires multiple runs)', 
                           ha='center', va='center', transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('Historical Performance')
            
            plt.tight_layout()
            
            # Save dashboard
            dashboard_file = os.path.join(self.output_dir, f'dashboard_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
            plt.savefig(dashboard_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            return dashboard_file
            
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            return None
    
    def _get_top_items(self, items: List[str], top_n: int = 10) -> List[tuple]:
        """Get top N most frequent items"""
        item_counts = self._count_items(items)
        return sorted(item_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    def _count_items(self, items: List[str]) -> Dict[str, int]:
        """Count occurrences of items"""
        counts = {}
        for item in items:
            if item:
                counts[item] = counts.get(item, 0) + 1
        return counts

def run_monitoring_report(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the monitoring and reporting process
    
    Args:
        config: Dictionary containing configuration parameters
        
    Returns:
        Dictionary containing results and report paths
    """
    try:
        report_generator = ReportGenerator(config.get('REPORTS_OUTPUT_DIR', 'reports'))
        
        # Get pipeline results from config
        pipeline_results = config.get('pipeline_results', {})
        
        if not pipeline_results:
            return {
                'success': False,
                'error': "No pipeline results provided for reporting"
            }
        
        # Generate comprehensive report
        report_result = report_generator.generate_pipeline_report(pipeline_results)
        
        if report_result['success']:
            logger.info(f"Monitoring report generated successfully")
            logger.info(f"Report file: {report_result['report_file']}")
            if report_result.get('dashboard_file'):
                logger.info(f"Dashboard: {report_result['dashboard_file']}")
            
            return {
                'success': True,
                'report_file': report_result['report_file'],
                'dashboard_file': report_result.get('dashboard_file'),
                'summary': report_result['summary']
            }
        else:
            return report_result
            
    except Exception as e:
        logger.error(f"Error in monitoring and reporting: {e}")
        return {
            'success': False,
            'error': str(e)
        }