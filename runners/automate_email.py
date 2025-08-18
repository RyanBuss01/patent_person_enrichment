# =============================================================================
# runners/automate_email.py
# Step 4: Automating Email Outreach
# =============================================================================
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, Any, List
import time
from jinja2 import Template
import os

logger = logging.getLogger(__name__)

class EmailAutomation:
    """Automate email outreach to enriched patent leads"""
    
    def __init__(self, smtp_server: str, smtp_port: int, email: str, password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email = email
        self.password = password
        
    def load_email_template(self, template_path: str) -> Template:
        """Load email template from file"""
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                template_content = f.read()
            return Template(template_content)
        except Exception as e:
            logger.error(f"Error loading template {template_path}: {e}")
            return None
    
    def create_default_template(self) -> Template:
        """Create a default email template if none exists"""
        default_template = """
Subject: Innovation Partnership Opportunity - {{ patent_title }}

Dear {{ recipient_name }},

I hope this email finds you well. I came across your recent patent "{{ patent_title }}" (Patent #{{ patent_number }}) and was impressed by your innovative work in this field.

{% if current_company %}
I see you're currently {{ current_title }} at {{ current_company }}, which aligns perfectly with some exciting opportunities we have.
{% endif %}

Our company specializes in helping innovators like yourself bring cutting-edge technologies to market. We've worked with numerous patent holders to:

• Accelerate product development and commercialization
• Connect with strategic partners and investors  
• Navigate regulatory and market entry challenges
• Maximize the value of intellectual property

Given your expertise in {{ patent_domain }}, I believe there could be significant synergy between your innovations and our capabilities.

Would you be open to a brief 15-minute conversation to explore potential collaboration opportunities? I'm happy to work around your schedule.

Best regards,
{{ sender_name }}
{{ sender_title }}
{{ company_name }}
{{ sender_email }}
{{ sender_phone }}

---
To unsubscribe from future emails, reply with "UNSUBSCRIBE" in the subject line.
        """.strip()
        
        return Template(default_template)
    
    def personalize_email(self, template: Template, enriched_person: Dict, config: Dict) -> Dict[str, str]:
        """Personalize email content for a specific person"""
        try:
            # Extract data
            pdl_data = enriched_person.get('enriched_data', {}).get('pdl_data', {})
            original_data = enriched_person.get('enriched_data', {}).get('original_data', {})
            
            # Determine recipient name
            recipient_name = pdl_data.get('full_name') or f"{original_data.get('first_name', '')} {original_data.get('last_name', '')}".strip()
            if not recipient_name:
                recipient_name = enriched_person.get('original_name', 'Innovator')
            
            # Extract patent domain from title (simplified)
            patent_title = enriched_person.get('patent_title', '')
            patent_domain = self._extract_domain_from_title(patent_title)
            
            # Template variables
            template_vars = {
                'recipient_name': recipient_name,
                'patent_title': patent_title,
                'patent_number': enriched_person.get('patent_number', ''),
                'patent_domain': patent_domain,
                'current_title': pdl_data.get('job_title', ''),
                'current_company': pdl_data.get('job_company_name', ''),
                'sender_name': config.get('SENDER_NAME', 'Business Development'),
                'sender_title': config.get('SENDER_TITLE', 'Partnership Manager'),
                'company_name': config.get('COMPANY_NAME', 'Your Company'),
                'sender_email': config.get('SENDER_EMAIL', self.email),
                'sender_phone': config.get('SENDER_PHONE', '')
            }
            
            # Render template
            rendered_content = template.render(**template_vars)
            
            # Split subject and body
            lines = rendered_content.split('\n')
            subject_line = ""
            body_lines = []
            
            for line in lines:
                if line.startswith('Subject: '):
                    subject_line = line.replace('Subject: ', '').strip()
                else:
                    body_lines.append(line)
            
            body = '\n'.join(body_lines).strip()
            
            return {
                'subject': subject_line,
                'body': body,
                'recipient_email': pdl_data.get('emails', [None])[0],
                'recipient_name': recipient_name
            }
            
        except Exception as e:
            logger.error(f"Error personalizing email for {enriched_person.get('original_name')}: {e}")
            return None
    
    def _extract_domain_from_title(self, title: str) -> str:
        """Extract technology domain from patent title"""
        title_lower = title.lower()
        
        domains = {
            'artificial intelligence': ['artificial intelligence', 'machine learning', 'neural network', 'ai'],
            'biotechnology': ['bio', 'genetic', 'dna', 'protein', 'pharmaceutical'],
            'electronics': ['electronic', 'circuit', 'semiconductor', 'chip'],
            'software': ['software', 'algorithm', 'computer', 'data processing'],
            'medical devices': ['medical', 'diagnostic', 'therapeutic', 'surgical'],
            'automotive': ['vehicle', 'automotive', 'car', 'engine'],
            'telecommunications': ['communication', 'wireless', 'network', 'signal'],
            'energy': ['solar', 'battery', 'energy', 'power', 'fuel']
        }
        
        for domain, keywords in domains.items():
            if any(keyword in title_lower for keyword in keywords):
                return domain
        
        return 'technology'
    
    def send_email(self, email_content: Dict, recipient_email: str) -> bool:
        """Send a single email"""
        if not recipient_email:
            logger.warning("No recipient email address")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.email
            msg['To'] = recipient_email
            msg['Subject'] = email_content['subject']
            
            # Add body
            msg.attach(MIMEText(email_content['body'], 'plain'))
            
            # Connect and send
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email, self.password)
                server.send_message(msg)
            
            logger.info(f"Email sent successfully to {recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {recipient_email}: {e}")
            return False
    
    def send_batch_emails(self, enriched_data: List[Dict], template: Template, config: Dict) -> Dict[str, int]:
        """Send emails to a batch of enriched leads"""
        results = {
            'sent': 0,
            'failed': 0,
            'no_email': 0
        }
        
        for person in enriched_data:
            # Personalize email
            email_content = self.personalize_email(template, person, config)
            
            if not email_content:
                results['failed'] += 1
                continue
            
            recipient_email = email_content.get('recipient_email')
            if not recipient_email:
                results['no_email'] += 1
                logger.warning(f"No email address for {person.get('original_name')}")
                continue
            
            # Send email
            if self.send_email(email_content, recipient_email):
                results['sent'] += 1
            else:
                results['failed'] += 1
            
            # Rate limiting to avoid spam filters
            time.sleep(config.get('EMAIL_DELAY_SECONDS', 2))
        
        return results

def run_email_automation(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the email automation process
    
    Args:
        config: Dictionary containing configuration parameters
        
    Returns:
        Dictionary containing results and statistics
    """
    try:
        # Initialize email automation
        emailer = EmailAutomation(
            smtp_server=config['SMTP_SERVER'],
            smtp_port=config['SMTP_PORT'],
            email=config['SENDER_EMAIL'],
            password=config['EMAIL_PASSWORD']
        )
        
        # Load or create template
        template_path = config.get('EMAIL_TEMPLATE_PATH')
        if template_path and os.path.exists(template_path):
            template = emailer.load_email_template(template_path)
        else:
            logger.info("Using default email template")
            template = emailer.create_default_template()
        
        if not template:
            return {
                'success': False,
                'error': "Failed to load email template",
                'emails_sent': 0,
                'emails_failed': 0
            }
        
        # Get enriched data
        enriched_data = config.get('enriched_data', [])
        if not enriched_data:
            return {
                'success': False,
                'error': "No enriched data provided",
                'emails_sent': 0,
                'emails_failed': 0
            }
        
        # Filter for people with email addresses
        people_with_emails = [
            person for person in enriched_data 
            if person.get('enriched_data', {}).get('pdl_data', {}).get('emails')
        ]
        
        logger.info(f"Found {len(people_with_emails)} people with email addresses out of {len(enriched_data)} total")
        
        if not people_with_emails:
            return {
                'success': False,
                'error': "No people with email addresses found",
                'emails_sent': 0,
                'emails_failed': 0
            }
        
        # Send emails
        results = emailer.send_batch_emails(people_with_emails, template, config)
        
        logger.info(f"Email automation completed. Sent: {results['sent']}, Failed: {results['failed']}, No email: {results['no_email']}")
        
        return {
            'success': True,
            'emails_sent': results['sent'],
            'emails_failed': results['failed'],
            'no_email_address': results['no_email'],
            'total_processed': len(enriched_data),
            'people_with_emails': len(people_with_emails)
        }
        
    except Exception as e:
        logger.error(f"Error in email automation: {e}")
        return {
            'success': False,
            'error': str(e),
            'emails_sent': 0,
            'emails_failed': 0
        }