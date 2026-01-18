"""Cryptographic utilities for data encryption and signing"""

import hashlib
import hmac
import json
import base64
from typing import Any, Dict
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class CryptoManager:
    """
    Handles encryption, decryption, and message signing
    """
    
    def __init__(self, encryption_key: str, signing_key: str):
        """
        Initialize crypto manager
        
        Args:
            encryption_key: Base64 encoded Fernet key
            signing_key: Key for HMAC signing
        """
        # Ensure key is proper length for Fernet
        if isinstance(encryption_key, str):
            encryption_key = encryption_key.strip()
        
        if encryption_key != 44:
            print(f"Deriving proper key from password/short key...")
            derived_bytes = self._derive_key(encryption_key)
            # Derive a proper key from the provided key
            encryption_key = self._derive_key(encryption_key)
            final_key = base64.urlsafe_b64encode(derived_bytes)
        else:
            final_key = encryption_key.encode() if isinstance(encryption_key, str) else encryption_key
        try:
            self.cipher = Fernet(final_key)
        except Exception as e:
            print(f"Critical: Fernet init failed with key length {len(final_key)}")
            raise e
        # Create Fernet cipher
        self.signing_key = signing_key.encode() if isinstance(signing_key, str) else signing_key
    
    @staticmethod
    def _derive_key(password: str, salt: bytes = b'sensor_sync_salt') -> bytes:
        """Derive a proper encryption key from password"""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(password.encode())
    
    @staticmethod
    def generate_encryption_key() -> str:
        """Generate a new Fernet encryption key"""
        return Fernet.generate_key().decode()
    
    def encrypt_field(self, data: Any) -> str:
        """
        Encrypt a single field
        
        Args:
            data: Data to encrypt (will be JSON serialized)
            
        Returns:
            Hex encoded encrypted data
        """
        if data is None:
            return None
        
        # Serialize to JSON
        json_data = json.dumps(data)
        
        # Encrypt
        encrypted = self.cipher.encrypt(json_data.encode())
        
        # Return as hex string
        return encrypted.hex()
    
    def decrypt_field(self, encrypted_hex: str) -> Any:
        """
        Decrypt a single field
        
        Args:
            encrypted_hex: Hex encoded encrypted data
            
        Returns:
            Original data (deserialized from JSON)
        """
        if not encrypted_hex:
            return None
        
        try:
            # Convert from hex
            encrypted_bytes = bytes.fromhex(encrypted_hex)
            
            # Decrypt
            decrypted = self.cipher.decrypt(encrypted_bytes)
            
            # Deserialize from JSON
            return json.loads(decrypted.decode())
        except Exception as e:
            raise ValueError(f"Decryption failed: {e}")
    
    def encrypt_sensitive_fields(
        self,
        data: Dict[str, Any],
        sensitive_fields: list
    ) -> Dict[str, Any]:
        """
        Encrypt specific fields in a dictionary
        
        Args:
            data: Dictionary containing data
            sensitive_fields: List of field names to encrypt
            
        Returns:
            Dictionary with encrypted fields
        """
        encrypted_data = data.copy()
        
        for field in sensitive_fields:
            if field in encrypted_data:
                original_value = encrypted_data[field]
                encrypted_data[field] = self.encrypt_field(original_value)
                encrypted_data[f'{field}_encrypted'] = True
        
        return encrypted_data
    
    def decrypt_sensitive_fields(
        self,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Decrypt fields marked as encrypted
        
        Args:
            data: Dictionary with encrypted fields
            
        Returns:
            Dictionary with decrypted fields
        """
        decrypted_data = data.copy()
        
        # Find encrypted fields
        encrypted_markers = [
            key for key in decrypted_data.keys()
            if key.endswith('_encrypted') and decrypted_data[key]
        ]
        
        for marker in encrypted_markers:
            field_name = marker.replace('_encrypted', '')
            if field_name in decrypted_data:
                encrypted_value = decrypted_data[field_name]
                decrypted_data[field_name] = self.decrypt_field(encrypted_value)
                del decrypted_data[marker]
        
        return decrypted_data
    
    def sign_message(self, message: Dict[str, Any]) -> str:
        """
        Create HMAC signature for a message
        
        Args:
            message: Message dictionary
            
        Returns:
            Hex encoded signature
        """
        # Serialize message to JSON with sorted keys for consistency
        message_str = json.dumps(message, sort_keys=True)
        
        # Create HMAC signature
        signature = hmac.new(
            self.signing_key,
            message_str.encode(),
            hashlib.sha256
        )
        
        return signature.hexdigest()
    
    def verify_signature(self, message: Dict[str, Any], signature: str) -> bool:
        """
        Verify HMAC signature of a message
        
        Args:
            message: Message dictionary
            signature: Signature to verify
            
        Returns:
            True if signature is valid
        """
        expected_signature = self.sign_message(message)
        return hmac.compare_digest(signature, expected_signature)
    
    def create_signed_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a signed message envelope
        
        Args:
            message: Message to sign
            
        Returns:
            Dictionary with message and signature
        """
        signature = self.sign_message(message)
        
        return {
            'message': message,
            'signature': signature,
            'version': '1.0'
        }
    
    def verify_signed_message(self, signed_message: Dict[str, Any]) -> bool:
        """
        Verify a signed message envelope
        
        Args:
            signed_message: Signed message dictionary
            
        Returns:
            True if signature is valid
        """
        message = signed_message.get('message')
        signature = signed_message.get('signature')
        
        if not message or not signature:
            return False
        
        return self.verify_signature(message, signature)