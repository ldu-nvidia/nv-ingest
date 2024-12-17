from collections import Counter
from typing import List, Dict, Any
from base64 import b64decode
from IPython.display import display, Image

def count_metadata_types(metadata: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Count the number of each metadata type in the generated metadata.
    """
    return dict(Counter(item['document_type'] for item in metadata))

def analyze_text_metadata(metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze text metadata and return key information.
    """
    text_items = [item for item in metadata if item['document_type'] == 'text']
    if not text_items:
        return {"error": "No text metadata found"}
    
    text_item = text_items[0]['metadata']
    return {
        "language": text_item['text_metadata']['language'],
        "page_count": text_item['content_metadata']['hierarchy']['page_count'],
        "source_name": text_item['source_metadata']['source_name'],
        "content_preview": text_item['content'][:200] + "..."  # First 200 characters
    }

def analyze_table_metadata(metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyze table metadata and return key information for each table.
    """
    table_items = [item['metadata'] for item in metadata 
                   if item['document_type'] == 'structured' 
                   and item['metadata']['content_metadata']['subtype'] == 'table']
    
    return [{
        "content_preview": table['table_metadata']['table_content'][:100] + "...",
        "location": table['table_metadata']['table_location'],
        "page_number": table['content_metadata']['page_number']
    } for table in table_items]

def analyze_chart_metadata(metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyze chart metadata and return key information for each chart.
    """
    chart_items = [item['metadata'] for item in metadata 
                   if item['document_type'] == 'structured' 
                   and item['metadata']['content_metadata']['subtype'] == 'chart']
    
    return [{
        "content_preview": chart['table_metadata']['table_content'][:100] + "...",
        "location": chart['table_metadata']['table_location'],
        "page_number": chart['content_metadata']['page_number']
    } for chart in chart_items]

def analyze_image_metadata(metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyze image metadata and return key information for each image.
    """
    image_items = [item['metadata'] for item in metadata if item['document_type'] == 'image']
    
    return [{
        "image_type": img['image_metadata']['image_type'],
        "dimensions": f"{img['image_metadata']['width']}x{img['image_metadata']['height']}",
        "location": img['image_metadata']['image_location'],
        "page_number": img['content_metadata']['page_number']
    } for img in image_items]

def display_image(metadata: List[Dict[str, Any]], index: int = 0):
    """
    Display an image from the metadata at the specified index.
    """
    image_items = [item for item in metadata if item['document_type'] == 'image']
    if index < 0 or index >= len(image_items):
        print(f"Invalid index. There are {len(image_items)} images.")
        return
    
    image_data = b64decode(image_items[index]['metadata']['content'])
    display(Image(image_data))

def comprehensive_metadata_analysis(metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Perform a comprehensive analysis of the metadata and return a summary.
    """
    return {
        "type_counts": count_metadata_types(metadata),
        "text_analysis": analyze_text_metadata(metadata),
        "table_analysis": analyze_table_metadata(metadata),
        "chart_analysis": analyze_chart_metadata(metadata),
        "image_analysis": analyze_image_metadata(metadata)
    }

def print_metadata_summary(analysis: Dict[str, Any]):
    """
    Print a formatted summary of the metadata analysis.
    """
    print("NV-Ingest Metadata Analysis Summary")
    print("===================================")
    
    print("\nMetadata Type Counts:")
    for doc_type, count in analysis['type_counts'].items():
        print(f"  {doc_type}: {count}")
    
    print("\nText Analysis:")
    text_analysis = analysis['text_analysis']
    print(f"  Language: {text_analysis['language']}")
    print(f"  Page Count: {text_analysis['page_count']}")
    print(f"  Source Name: {text_analysis['source_name']}")
    print(f"  Content Preview: {text_analysis['content_preview']}")
    
    print("\nTable Analysis:")
    for i, table in enumerate(analysis['table_analysis'], 1):
        print(f"  Table {i}:")
        print(f"    Content Preview: {table['content_preview']}")
        print(f"    Location: {table['location']}")
        print(f"    Page Number: {table['page_number']}")
    
    print("\nChart Analysis:")
    for i, chart in enumerate(analysis['chart_analysis'], 1):
        print(f"  Chart {i}:")
        print(f"    Content Preview: {chart['content_preview']}")
        print(f"    Location: {chart['location']}")
        print(f"    Page Number: {chart['page_number']}")
    
    print("\nImage Analysis:")
    for i, image in enumerate(analysis['image_analysis'], 1):
        print(f"  Image {i}:")
        print(f"    Type: {image['image_type']}")
        print(f"    Dimensions: {image['dimensions']}")
        print(f"    Location: {image['location']}")
        print(f"    Page Number: {image['page_number']}")


generated_meta_image = [{'image': "/home/ldu/Documents/Repos/nv-ingest/ASML_Snapdragon_parsed/image/snapdragon_600_apq_8064_data_sheet.pdf.metadata.json"}]
analysis = comprehensive_metadata_analysis(generated_meta_image)  # One analyze first file result
print_metadata_summary(analysis)

# To display an image:
display_image(generated_meta_image, 0)  # Display the first image