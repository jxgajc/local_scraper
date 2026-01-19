import hashlib
import logging

logger = logging.getLogger(__name__)

class BizFingerprintMixin:
    """
    业务指纹生成 Mixin
    用于统一生成基于业务字段的唯一标识 (md5_id)
    """
    
    def generate_biz_id(self, field_mapping=None):
        """
        生成业务唯一指纹 (MD5)
        基于: HospitalName, ProductName, MedicineModelName, Outlookc, Pack, Manufacturer
        
        :param field_mapping: 字段映射字典 {标准字段名: Item实际字段名}。
                              如果为 None，默认 Item 字段名与标准字段名一致。
        """
        # 定义业务唯一键字段 (根据 Phase 1 1.1 定义)
        biz_keys = [
            'HospitalName', 
            'ProductName', 
            'MedicineModelName', 
            'Outlookc', 
            'Pack', 
            'Manufacturer'
        ]
        
        # 提取值并标准化
        raw_values = []
        for key in biz_keys:
            # 确定 Item 中的字段名
            item_key = key
            if field_mapping and key in field_mapping:
                item_key = field_mapping[key]
                
            val = self.get(item_key)
            if val is None:
                val = ''
            else:
                val = str(val).strip()
            raw_values.append(val)
            
        # 拼接 (使用 || 分隔以避免边界混淆)
        raw_str = '||'.join(raw_values)
        
        # 生成 MD5
        md5_hash = hashlib.md5(raw_str.encode('utf-8')).hexdigest()
        
        # 赋值给 md5_id
        self['md5_id'] = md5_hash
        
        return md5_hash
