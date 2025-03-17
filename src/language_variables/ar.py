""" العربية """

language = 'ar'

novalue =  'لا قيمة'

time_variables = {
    'months': ['كانون الثاني', 'شباط', 'آذار', 'نيسان', 'أيار', 'حزيران', 'تموز', 'آب', 'أيلول', 'تشرين الأول', 'تشرين الثاني', 'كانون الأول'],
    'century': 'قرن',
    'millennium': 'ألفية',
    'decade': 'عقد',
    'AD': 'م',
    'BC': 'ق.م',
    'years': 'سنوات',
    'ten thousand years': 'عشرة آلاف سنة',
    'hundred thousand years': 'مئات آلاف السنين',
    'million years': 'ملايين السنين',
    'tens of millions of years': 'عشرات الملايين من السنين',
    'hundred million years': 'مئات الملايين من السنين',
    'billion years': 'مليار سنة',
}

def merge_entity_text(label, description, aliases, properties):
    """
    دمج خصائص الكائن (التسمية، الوصف، الألقاب، والخصائص) في نص واحد.

    المعلمات:
    - label: سلسلة تمثل تسمية الكائن.
    - description: سلسلة تمثل وصف الكائن.
    - aliases: قاموس الألقاب.
    - properties: قاموس الخصائص.

    الإرجاع:
    - تمثيل سلسلة للكائن، ووصفه، وتسمية، وألقابه، وادعاءاته. إذا لم توجد ادعاءات، ينتهي الوصف بنقطة.
    """
    text = f"{label}، {description}"

    if len(aliases) > 0:
        text += f"، المعروف أيضًا باسم {'، '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. السمات تتضمن: {properties_text}"
    else:
        text = f"{text}."

    return text

def qualifiers_to_text(qualifiers):
    """
    تحويل قائمة المؤهلات إلى سلسلة نصية قابلة للقراءة.
    توفر المؤهلات معلومات إضافية حول الادعاء.

    المعلمات:
    - qualifiers: قاموس من المؤهلات مع معرّفات الخصائص كمفاتيح وقيمها كقوائم.

    الإرجاع:
    - تمثيل سلسلة للمؤهلات.
    """
    text = ""
    for property_label, qualifier_values in qualifiers.items():
        if (qualifier_values is not None) and len(qualifier_values) > 0:
            if len(text) > 0:
                text += f" ; "

            text += f"{property_label}: {'، '.join(qualifier_values)}"

    if len(text) > 0:
        return f" ({text})"
    return ""

def properties_to_text(properties):
    """
    تحويل قائمة الخصائص (الادعاءات) إلى سلسلة نصية قابلة للقراءة.

    المعلمات:
    - properties: قاموس من الخصائص (الادعاءات) مع معرّفات الخصائص كمفاتيح.

    الإرجاع:
    - تمثيل سلسلة للخصائص وقيمها.
    """
    properties_text = ""
    for property_label, claim_values in properties.items():
        if (claim_values is not None) and (len(claim_values) > 0):

            claims_text = ""
            for claim_value in claim_values:
                if len(claims_text) > 0:
                    claims_text += f"،\n "

                claims_text += f"«{claim_value['value']}"

                qualifiers = claim_value.get('qualifiers', {})
                if len(qualifiers) > 0:
                    claims_text += qualifiers_to_text(qualifiers)

                claims_text += f"»"

            properties_text += f'\n- {property_label}: {claims_text}'

    return properties_text